import argparse
import configparser
from decimal import ROUND_DOWN, ROUND_FLOOR, Decimal


def compute_levels_quote(price_levels, req_base_amount=None, req_quote_amount=None):
    if req_base_amount and req_quote_amount:
        raise ValueError("Base amount and quote amount cannot both be specified")

    levels = to_price_levels_decimal(price_levels)
    if not levels:
        return {"failure": "insufficient_liquidity"}

    base_amount = levels[0]["level"]
    quote_amount = levels[0]["level"] * levels[0]["price"]

    if (
        req_base_amount
        and req_base_amount < base_amount
        or (req_quote_amount and req_quote_amount < quote_amount)
    ):
        return {"failure": "below_minimum_amount"}

    for i in range(1, len(levels)):
        next_level = levels[i]
        next_level_depth = next_level["level"] - levels[i - 1]["level"]
        next_level_quote = quote_amount + next_level_depth * next_level["price"]
        if req_base_amount and req_base_amount <= next_level["level"]:
            base_difference = req_base_amount - base_amount
            quote_amount = quote_amount + base_difference * next_level["price"]
            return {"amount": quote_amount}
        elif req_quote_amount and req_quote_amount <= next_level_quote:
            quote_difference = req_quote_amount - quote_amount
            base_amount = base_amount + quote_difference / next_level["price"]
            return {"amount": base_amount}
        base_amount = next_level["level"]
        quote_amount = next_level_quote

    return {"failure": "insufficient_liquidity"}


def round_precision(num, precision):
    if num == 0:
        return num

    left_of_decimal = int(
        abs(num).log10().quantize(Decimal("1."), rounding=ROUND_FLOOR) + 1
    )
    right_of_decimal = precision - left_of_decimal
    if right_of_decimal < 0:
        multiplier = 10**-right_of_decimal
        return (num / multiplier).quantize(Decimal("1.")) * multiplier
    else:
        return num.quantize(Decimal("1.".ljust(2 + right_of_decimal, "0")))


def get_dp(num):
    return max(str(num)[::-1].find("."), 0)


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


def extract_expected_amount(levels, baseAmount, quoteAmount):
    result = compute_levels_quote(levels, baseAmount, quoteAmount)
    if not result.get("failure") and result.get("amount"):
        return result["amount"]
    else:
        return None


def convert_from_decimals(amount, token):
    return Decimal(amount) / (10 ** Decimal(token["decimals"]))


def convert_to_decimals(amount, token):
    return (amount * (Decimal(10) ** token["decimals"])).quantize(
        Decimal("1."), rounding=ROUND_DOWN
    )


def to_price_levels_decimal(price_levels, options=None):
    invert = options.get("invert") if options else False
    return [
        {
            "level": Decimal(l["level"]),
            "price": 1 / Decimal(l["price"]) if invert else Decimal(l["price"]),
        }
        for l in price_levels
    ]
