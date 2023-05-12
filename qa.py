#!/usr/bin/env python3
import argparse
import asyncio
import configparser
from decimal import ROUND_DOWN, ROUND_UP, Decimal
import json
from random import random
from hashflow.helpers.validation import validate_evm_address, validate_chain_id
from hashflow.api import HashflowApi
from validations import validate_maker_name
import sys


def get_config(config_file):
    config = configparser.ConfigParser()
    config.read(config_file)
    return {
        "auth_key": config["general"]["auth_key"],
        "qa_taker_address": config["general"]["qa_taker_address"],
    }


def get_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--maker", required=True)
    parser.add_argument("--chain", required=True, type=int)
    parser.add_argument("--base_token", default=None)
    parser.add_argument("--quote_token", default=None)
    parser.add_argument(
        "--env", default="staging", choices=["staging", "production", "development"]
    )
    parser.add_argument("--num_requests", default=30, type=int)
    parser.add_argument("--delay_ms", default=0, type=int)
    parser.add_argument("--config", default="config.ini")

    return parser.parse_args()


async def handler(args, options):
    validate_evm_address(options["qa_taker_address"])
    validate_maker_name(args.maker)
    validate_chain_id(args.chain)

    async with HashflowApi("taker", "qa", options["auth_key"], args.env) as api:
        pair_provided = args.base_token is not None and args.quote_token is not None

        pair_str = f" for {args.base_token}-{args.quote_token}" if pair_provided else ""
        more_str = (
            f" with {args.num_requests} requests/pair" if args.num_requests else ""
        )
        if args.delay_ms:
            more_str += f" and {args.delay_ms}ms delay"

        sys.stdout.write(
            f"QA testing maker '{args.maker}' against {args.env} on chain {args.chain}{pair_str}{more_str}.\n\n"
        )

        sys.stdout.write("Finding active makers ... ")

        try:
            chain_makers = await api.get_market_makers(
                chain_id=args.chain, market_maker=args.maker
            )

            def check_maker(maker):
                prefix = maker.split("_")[0]
                return prefix == args.maker

            makers = list(filter(check_maker, chain_makers))
            if len(makers) == 0:
                raise Exception(f"No makers available: {chain_makers}")

        except Exception as e:
            sys.stdout.write(f"Failed! {e}\n")
            exit(-1)

        makers_list_or_one = makers if len(makers) > 1 else makers[0]
        sys.stdout.write(f"done. {makers_list_or_one}\n")
        sys.stdout.write(f"Fetching levels for {makers_list_or_one}...")

        try:
            levels = await api.get_price_levels(args.chain, makers)
            if not levels.keys():
                raise Exception("No maker levels.\n")

            def filter_fn(maker):
                def filter_levels(entry):
                    pair = entry["pair"]
                    if pair_provided:
                        pair_base_token_name = pair["baseTokenName"]
                        pair_quote_token_name = pair["quoteTokenName"]
                        if pair_provided and not (
                            pair_base_token_name == args.base_token
                            and pair_quote_token_name == args.quote_token
                        ):
                            return False
                    levels_data = entry["levels"]
                    if not levels_data:
                        sys.stdout.write(
                            f" No levels for {maker} on {pair_base_token_name}-{pair_quote_token_name}. Continuing with next pair...\n"
                        )
                        return False
                    return True

                return filter_levels

            def transform_levels(entry):
                pair = entry["pair"]
                pair_base_token_name = pair["baseTokenName"]
                pair_quote_token_name = pair["quoteTokenName"]
                base_token = {
                    "chainId": args.chain,
                    "address": pair["baseToken"],
                    "name": pair_base_token_name,
                    "decimals": pair["baseTokenDecimals"],
                }
                quote_token = {
                    "chainId": args.chain,
                    "address": pair["quoteToken"],
                    "name": pair_quote_token_name,
                    "decimals": pair["quoteTokenDecimals"],
                }
                return {
                    "baseToken": base_token,
                    "quoteToken": quote_token,
                    "levels": entry["levels"],
                }

            maker_levels = {
                m: list(map(transform_levels, filter(filter_fn(m), l)))
                for m, l in levels.items()
            }
            sys.stdout.write("done\n")
            for maker, levels in maker_levels.items():
                for entry in levels:
                    base_name = entry["baseToken"]["name"]
                    quote_name = entry["quoteToken"]["name"]
                    pair_str = f"{base_name}-{quote_name}"
                    sys.stdout.write(f"Requesting RFQs for {maker}: {pair_str} ... ")

                    try:
                        result = await testRfqs(
                            api,
                            options["qa_taker_address"],
                            args.num_requests,
                            args.delay_ms,
                            maker,
                            args.chain,
                            entry,
                        )
                        # successRate = result['successRate']
                        # biasBps = result['biasBps']
                        # deviationBps = result['deviationBps']
                        # results = result['results']
                        # Here we put in the proper output code
                        sys.stdout.write(f"results for {maker}/{pair_str}: {result}\n")
                    except Exception as e:
                        print(f"Failed to get RFQs for {maker}: {pair_str}")
                        raise e

        except Exception as e:
            sys.stdout.write(f"failed! {e}\n")
            raise e
            sys.exit(-1)


async def testRfqs(api, wallet, numRequests, delayMs, maker, chainId, entry):
    numSuccess = 0
    sumBiasBps = 0
    deviationEntries = []
    # Compute min and max levels
    preLevels = entry["levels"]

    if len(preLevels) == 1:
        raise ValueError(f"Levels for {maker} only have one entry: {entry}")

    minLevel = Decimal(preLevels[0]["level"] or "0")
    maxLevel = Decimal.max(
        Decimal(preLevels[-1]["level"] or "0") * Decimal("0.95"), minLevel
    )

    async def sendRfq():
        baseToken, quoteToken = entry["baseToken"], entry["quoteToken"]
        provided = "base" if random() < 0.5 else "quote"
        baseAmount = Decimal(random()) * (maxLevel - minLevel) + minLevel
        levelsQuote = compute_levels_quote(preLevels, baseAmount)
        if levelsQuote.get("failure") or not levelsQuote["amount"]:
            failMsg = f"Could not estimate pre-RFQ prices: {levelsQuote['failure']}. {json.dumps(preLevels)}"
            return {
                "provided": provided,
                "baseAmount": baseAmount,
                "quoteAmount": None,
                "failMsg": failMsg,
            }

        quoteAmount = levelsQuote["amount"]
        baseTokenAmount, quoteTokenAmount = (
            (convert_to_decimals(baseAmount, baseToken), None)
            if provided == "base"
            else (None, convert_to_decimals(quoteAmount, quoteToken))
        )
        feeBps = round(random() * 10)
        feeFactor = 1 - Decimal(feeBps) / 10000
        try:
            levelsMap, rfq = await asyncio.gather(
                api.get_price_levels(chainId, [maker]),
                api.request_quote(
                    chain_id=chainId,
                    base_token=baseToken["address"],
                    quote_token=quoteToken["address"],
                    base_token_amount=str(baseTokenAmount) if baseTokenAmount else None,
                    quote_token_amount=str(quoteTokenAmount) if quoteTokenAmount else None,
                    wallet=wallet,
                    market_makers=[maker],
                    feeBps=feeBps,
                    debug=True,
                ),
            )
            levels = next(
                (
                    e["levels"]
                    for e in levelsMap[maker]
                    if e["pair"]["baseToken"] == baseToken["address"]
                    and e["pair"]["quoteToken"] == quoteToken["address"]
                ),
                None,
            )
            if not levels:
                return {
                    "provided": provided,
                    "baseAmount": baseAmount,
                    "quoteAmount": quoteAmount,
                    "feeBps": feeBps,
                    "failMsg": f"No levels for {maker}. Received: {json.dumps(levelsMap)}",
                }
            expectedToken = quoteToken if provided == "base" else baseToken
            expectedAmount = (
                extract_expected_amount(levels, baseAmount, None) * feeFactor
                if provided == "base"
                else extract_expected_amount(levels, None, quoteAmount) / feeFactor
            )
            if not expectedAmount:
                return {
                    "provided": provided,
                    "baseAmount": baseAmount if provided == "base" else None,
                    "quoteAmount": quoteAmount if provided == "quote" else None,
                    "failMsg": f"Could not estimate post-RFQ prices: {failure}. {json.dumps(levels)}",
                }
            expectedAmountDecimals = convert_to_decimals(expectedAmount, expectedToken)
            if rfq.get("quoteData") is None:
                return {
                    "provided": provided,
                    "baseAmount": baseAmount,
                    "quoteAmount": quoteAmount,
                    "expectedAmount": expectedAmount,
                    "feeBps": feeBps,
                    "rfqIds": rfq["internalRfqIds"] or [],
                    "failMsg": f"No quote data. Received error: {json.dumps(rfq['error'])}",
                }
            receivedAmountDecimals = Decimal(
                rfq["quoteData"]["quoteTokenAmount"]
                if provided == "base"
                else rfq["quoteData"]["baseTokenAmount"] or "0"
            )
            receivedAmount = convert_from_decimals(
                receivedAmountDecimals, expectedToken
            )
            deviationFactor = -1 if provided == "base" else 1
            deviationBps = (
                (receivedAmountDecimals - expectedAmountDecimals)
                * deviationFactor
                / expectedAmountDecimals
                * 100
            )
            if deviationBps.is_nan() or deviationBps.is_zero():
                deviationBps = Decimal(0)

            # Compute success stats
            # numSuccess += 1
            # sumBiasBps += deviationBps.to_number()
            deviationEntries.append(deviationBps)

            return {
                "provided": provided,
                "baseAmount": baseAmount if provided == "base" else receivedAmount,
                "quoteAmount": quoteAmount if provided == "quote" else receivedAmount,
                "expectedAmount": expectedAmount,
                "deviationBps": deviationBps,
                "feeBps": feeBps,
                "rfqIds": rfq.get("internalRfqIds", []),
            }

        except Exception as e:
            raise e
            return {"provided": provided, "failMsg": f"Error occurred: {e}"}

    resultFutures = []
    for i in range(0, numRequests):
        if delayMs > 0:
            await asyncio.sleep(delayMs / 1000)
        resultFutures.append(sendRfq())

    results = await asyncio.gather(*resultFutures)

    successes = [result for result in results if result.get("failMsg") is None]
    numSuccess = len(successes)
    if not numSuccess:
        return {
            "successRate": 0,
            "results": results
        }
    deviationEntries = [result["deviationBps"] for result in successes]
    sumBiasBps = sum(deviationEntries)
    bias_bps = sumBiasBps / numSuccess
    sum_squared_deviation = sum([(d - Decimal(bias_bps)) ** 2 for d in deviationEntries])
    deviation_bps = (sum_squared_deviation**numSuccess).sqrt()

    return {
        "successRate": numSuccess / numRequests,
        "biasBps": bias_bps,
        "deviationBps": deviation_bps,
        "results": results,
    }


def compute_levels_quote(price_levels, req_base_amount=None, req_quote_amount=None):
    if req_base_amount and req_quote_amount:
        raise ValueError("Base amount and quote amount cannot both be specified")

    levels = to_price_levels_decimal(price_levels)
    if not levels:
        return {"failure": "insufficient_liquidity"}

    quote = {
        "baseAmount": levels[0]["level"],
        "quoteAmount": levels[0]["level"] * levels[0]["price"],
    }
    if (
        req_base_amount
        and req_base_amount < quote["baseAmount"]
        or (req_quote_amount and req_quote_amount < quote["quoteAmount"])
    ):
        return {"failure": "below_minimum_amount"}

    for i in range(1, len(levels)):
        next_level = levels[i]
        next_level_depth = next_level["level"] - levels[i - 1]["level"]
        next_level_quote = quote["quoteAmount"] + next_level_depth * next_level["price"]
        if req_base_amount and req_base_amount <= next_level["level"]:
            base_difference = req_base_amount - quote["baseAmount"]
            quote_amount = quote["quoteAmount"] + base_difference * next_level["price"]
            return {"amount": quote_amount}
        elif req_quote_amount and req_quote_amount <= next_level_quote:
            quote_difference = req_quote_amount - quote["quoteAmount"]
            base_amount = quote["baseAmount"] + quote_difference / next_level["price"]
            return {"amount": base_amount}
        quote["baseAmount"] = next_level["level"]
        quote["quoteAmount"] = next_level_quote

    return {"failure": "insufficient_liquidity"}


def to_price_levels_decimal(price_levels, options=None):
    invert = options.get("invert") if options else False
    return [
        {
            "level": Decimal(l["level"]),
            "price": 1 / Decimal(l["price"])
            if invert
            else Decimal(l["price"]),
        }
        for l in price_levels
    ]


def convert_to_decimals(amount, token):
    return (amount * (Decimal(10) ** token["decimals"])).quantize(Decimal('1.'), rounding=ROUND_DOWN)


def convert_from_decimals(amount, token):
    return Decimal(amount) / (10 ** Decimal(token["decimals"]))


def extract_expected_amount(levels, baseAmount, quoteAmount):
    result = compute_levels_quote(levels, baseAmount, quoteAmount)
    if not result.get("failure") and result.get("amount"):
        return result["amount"]
    else:
        return None


if __name__ == "__main__":
    args = get_args()
    options = get_config(args.config)
    asyncio.run(handler(args, options))
