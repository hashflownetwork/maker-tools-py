#!/usr/bin/env python3
import argparse
import configparser
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
    parser.add_argument("--env", default="staging", choices=["staging", "production", "development"])
    parser.add_argument("--num_requests", default=30, type=int)
    parser.add_argument("--delay_ms", default=0, type=int)
    parser.add_argument("--config", default="config.ini")

    return parser.parse_args()

def handler(args, options):
    validate_evm_address(options['qa_taker_address'])
    validate_maker_name(args.maker)
    validate_chain_id(args.chain)

    api = HashflowApi('taker', 'qa', options['auth_key'], args.env)
    pair_provided = args.base_token is not None and args.quote_token is not None


    pair_str = f" for {args.base_token}-{args.quote_token}" if pair_provided else ''
    more_str = f" with {args.num_requests} requests/pair" if args.num_requests else ''
    if args.delay_ms:
        more_str += f" and {args.delay_ms}ms delay"

    sys.stdout.write(
        f"QA testing maker '{args.maker}' against {args.env} on chain {args.chain}{pair_str}{more_str}.\n\n"
    )

    sys.stdout.write('Finding active makers ... ')

    chain_makers = api.get_market_makers(args.chain, None, args.maker)
    def check_maker(maker): 
        prefix = maker.split('_')[0]
        return prefix == args.maker
    
    makers =list(filter(check_maker, chain_makers))

    print(makers)

if __name__ == "__main__":
    args = get_args()
    options = get_config(args.config)
    handler(args, options)