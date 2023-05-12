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


async def testRfqs(api, wallet, num_requests, delay_ms, maker, chain_id, entry):
    # Compute min and max levels
    pre_levels = entry["levels"]

    if len(pre_levels) == 1:
        raise ValueError(f"Levels for {maker} only have one entry: {entry}")

    min_level = Decimal(pre_levels[0]["level"] or "0")
    max_level = Decimal.max(
        Decimal(pre_levels[-1]["level"] or "0") * Decimal("0.95"), min_level
    )

    async def send_rfq():
        base_token, quote_token = entry["baseToken"], entry["quoteToken"]
        provided = "base" if random() < 0.5 else "quote"
        base_amount = Decimal(random()) * (max_level - min_level) + min_level
        levels_quote = compute_levels_quote(pre_levels, base_amount)
        if levels_quote.get("failure") or not levels_quote["amount"]:
            fail_msg = f"Could not estimate pre-RFQ prices: {levels_quote['failure']}. {json.dumps(pre_levels)}"
            return {
                "provided": provided,
                "baseAmount": base_amount,
                "quoteAmount": None,
                "failMsg": fail_msg,
            }

        quote_amount = levels_quote["amount"]
        base_token_amount, quote_token_amount = (
            (convert_to_decimals(base_amount, base_token), None)
            if provided == "base"
            else (None, convert_to_decimals(quote_amount, quote_token))
        )
        fee_bps = round(random() * 10)
        fee_factor = 1 - Decimal(fee_bps) / 10000
        try:
            levels_map, rfq = await asyncio.gather(
                api.get_price_levels(chain_id, [maker]),
                api.request_quote(
                    chain_id=chain_id,
                    base_token=base_token["address"],
                    quote_token=quote_token["address"],
                    base_token_amount=str(base_token_amount) if base_token_amount else None,
                    quote_token_amount=str(quote_token_amount) if quote_token_amount else None,
                    wallet=wallet,
                    market_makers=[maker],
                    feeBps=fee_bps,
                    debug=True,
                ),
            )
            levels = next(
                (
                    e["levels"]
                    for e in levels_map[maker]
                    if e["pair"]["baseToken"] == base_token["address"]
                    and e["pair"]["quoteToken"] == quote_token["address"]
                ),
                None,
            )
            if not levels:
                return {
                    "provided": provided,
                    "baseAmount": base_amount,
                    "quoteAmount": quote_amount,
                    "feeBps": fee_bps,
                    "failMsg": f"No levels for {maker}. Received: {json.dumps(levels_map)}",
                }
            expected_token = quote_token if provided == "base" else base_token
            expected_amount = (
                extract_expected_amount(levels, base_amount, None) * fee_factor
                if provided == "base"
                else extract_expected_amount(levels, None, quote_amount) / fee_factor
            )
            if not expected_amount:
                return {
                    "provided": provided,
                    "baseAmount": base_amount if provided == "base" else None,
                    "quoteAmount": quote_amount if provided == "quote" else None,
                    "failMsg": f"Could not estimate post-RFQ prices: {failure}. {json.dumps(levels)}",
                }
            expected_amount_decimals = convert_to_decimals(expected_amount, expected_token)
            if rfq.get("quoteData") is None:
                return {
                    "provided": provided,
                    "baseAmount": base_amount,
                    "quoteAmount": quote_amount,
                    "expectedAmount": expected_amount,
                    "feeBps": fee_bps,
                    "rfqIds": rfq["internalRfqIds"] or [],
                    "failMsg": f"No quote data. Received error: {json.dumps(rfq['error'])}",
                }
            received_amount_decimals = Decimal(
                rfq["quoteData"]["quoteTokenAmount"]
                if provided == "base"
                else rfq["quoteData"]["baseTokenAmount"] or "0"
            )
            received_amount = convert_from_decimals(
                received_amount_decimals, expected_token
            )
            deviation_factor = -1 if provided == "base" else 1
            deviation_bps = (
                (received_amount_decimals - expected_amount_decimals)
                * deviation_factor
                / expected_amount_decimals
                * 100
            )
            if deviation_bps.is_nan() or deviation_bps.is_zero():
                deviation_bps = Decimal(0)

            return {
                "provided": provided,
                "baseAmount": base_amount if provided == "base" else received_amount,
                "quoteAmount": quote_amount if provided == "quote" else received_amount,
                "expectedAmount": expected_amount,
                "deviationBps": deviation_bps,
                "feeBps": fee_bps,
                "rfqIds": rfq.get("internalRfqIds", []),
            }

        except Exception as e:
            raise e
            return {"provided": provided, "failMsg": f"Error occurred: {e}"}

    result_futures = []
    for i in range(0, num_requests):
        if delay_ms > 0:
            await asyncio.sleep(delay_ms / 1000)
        result_futures.append(send_rfq())

    results = await asyncio.gather(*result_futures)

    successes = [result for result in results if result.get("failMsg") is None]
    num_success = len(successes)
    if not num_success:
        return {
            "successRate": 0,
            "results": results
        }
    deviation_entries = [result["deviationBps"] for result in successes]
    sum_bias_bps = sum(deviation_entries)
    bias_bps = sum_bias_bps / num_success
    sum_squared_deviation = sum([(d - Decimal(bias_bps)) ** 2 for d in deviation_entries])
    deviation_bps = (sum_squared_deviation**num_success).sqrt()

    return {
        "successRate": num_success / num_requests,
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
