#!/usr/bin/env python
"""
Invocation (parameters) examples:
    top_user --timeline_start 2016/01/01 --timeline_end 2016/12/31
    general_sentiment --user_id 592843104

    detect_language --tweet_id 776655406764613632
"""
import argparse
import logging
import os
import signal
import sys
from argparse import Namespace

from pyconfr_2019.grpc_nlp.protos import TweetFeaturesService_pb2_grpc
from pyconfr_2019.grpc_nlp.tools.fct_logger import init_logger
from pyconfr_2019.grpc_nlp.tools.fct_parser import EndDateType, StartDateType
from pyconfr_2019.grpc_nlp.tools.rpc_init_stub import rpc_init_stub

from features_processing.commands import (
    detect_language_command,
    general_sentiment_command,
    top_user_command
)

logger = logging.getLogger(__name__)

SIGNALS = [signal.SIGINT, signal.SIGTERM]


def build_parser(parser=None, **argparse_options):
    """
    Args:
        parser (argparse.ArgumentParser):
        **argparse_options (dict):
    Returns:
    """
    if parser is None:
        parser = argparse.ArgumentParser(**argparse_options)

    argparse_default = "(default=%(default)s)."

    # GRPC SERVICES
    # - Features Processor
    parser.add_argument("--twitter_analyzer_features_addr",
                        default=os.environ.get('TWITTER_ANALYZER_FEATURES_SERVICE_HOST',
                                               'localhost'),
                        type=str,
                        help="")
    parser.add_argument("--twitter_analyzer_features_port",
                        default=int(os.environ.get('TWITTER_ANALYZER_FEATURES_PORT',
                                                   '50051')),
                        type=int,
                        help="(default=%(default)s).",
                        metavar='')

    subparsers = parser.add_subparsers(help='Features processing',
                                       dest="command")

    # create the parser for the "top_user" command
    parser_top_user = subparsers.add_parser(
        'top_user',
        help='Display top users from twitter timeline.')
    parser_top_user.add_argument(
        '-ts', '--timeline_start',
        dest='timeline_start',
        type=StartDateType(),
        required=True,
        help="The start date of the timeline.",
    )
    parser_top_user.add_argument(
        '-te', '--timeline_end',
        dest='timeline_end',
        type=EndDateType(),
        required=True,
        help="The end date of the timeline.",
    )
    # create the parser for the "general_sentiment" command
    parser_general_sentiment = subparsers.add_parser(
        'general_sentiment',
        help='Compute the general sentiment for a twitter user.'
    )
    parser_general_sentiment.add_argument(
        '-ui', '--user_id',
        dest='user_id',
        type=int,
        required=True,
        help="Twitter User id"
    )
    # create the parser for the "detect_language" command
    parser_detect_language = subparsers.add_parser(
        'detect_language',
        help='Detect language from tweet text.'
    )
    parser_detect_language.add_argument(
        '-ti', '--tweet_id',
        dest='tweet_id',
        type=int,
        required=True,
        help="Twitter User id"
    )

    parser.add_argument(
        '-ll', '--log_level',
        type=str, required=False, default='debug',
        choices=['debug', 'warning', 'info', 'error', 'critical'],
        help=f"The logger filter level. {argparse_default}",
    )
    parser.add_argument(
        '-lf', '--log_file',
        type=str, required=False,
        help="The path to the file into which the logs will be streamed. "
             f"{argparse_default}",
    )

    parser.add_argument("-v", "--verbose",
                        action="store_true", default=False,
                        help="increase output verbosity (enable 'DEBUG' level log). "
                             f"{argparse_default}")

    return parser


def parse_arguments(args=None):
    """
    Returns:
        # argparse.Namespace:
    """
    # return parsing
    return build_parser().parse_args(args)


def process(args: Namespace):
    def _signal_handler(_sig, _):
        """ Empty signal handler used to override python default one """
        logger.info("sig: {} intercepted. Closing application.".format(_sig))
        # https://stackoverflow.com/questions/73663/terminating-a-python-script
        sys.exit()

    # Signals HANDLER (to exit properly)
    for sig in SIGNALS:
        signal.signal(sig, _signal_handler)

    # Init gRPC services
    # - Features Processor
    features_rpc_stub = rpc_init_stub(
        args.twitter_analyzer_features_addr,
        args.twitter_analyzer_features_port,
        TweetFeaturesService_pb2_grpc.TweetFeaturesServiceStub,
        service_name='twitter analyzer features processor')

    if args.command == 'top_user':
        top_user_command(features_rpc_stub, args.timeline_start, args.timeline_end)
    elif args.command == 'general_sentiment':
        general_sentiment_command(features_rpc_stub, args.user_id)
    elif args.command == 'detect_language':
        detect_language_command(features_rpc_stub, args.tweet_id)


def main(args=None):
    # Deal with inputs (stdin, parameters, etc ...)
    args = parse_arguments(args)

    init_logger(args.log_level)

    process(args)


if __name__ == '__main__':
    main()
    sys.exit(0)
