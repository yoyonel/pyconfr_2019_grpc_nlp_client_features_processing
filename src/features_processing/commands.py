import logging

from google.protobuf.json_format import MessageToDict
from pyconfr_2019.grpc_nlp.protos.Timeline_pb2 import Timeline
from pyconfr_2019.grpc_nlp.protos.TweetFeaturesService_pb2 import (
    ComputeGeneralSentimentOfUserRequest,
    DetectLanguageFromTweetTextRequest,
    TopUsersRequest
)

logger = logging.getLogger(__name__)


def top_user_command(features_rpc_stub, timeline_start, timeline_end):
    top_user_response = features_rpc_stub.TopUsers(
        TopUsersRequest(
            timeline=Timeline(start=int(timeline_start.timestamp() * 1000),
                              end=int(timeline_end.timestamp() * 1000)))
    )
    for i, top_user in enumerate(top_user_response, start=1):
        logger.info(f"#{i} - user_id={top_user.user_id}, nb_tweets={top_user.nb_tweets}")


def general_sentiment_command(features_rpc_stub, user_id):
    general_sentiment_of_user_response = features_rpc_stub.ComputeGeneralSentimentOfUser(
        ComputeGeneralSentimentOfUserRequest(user_id=user_id))
    general_sentiment = general_sentiment_of_user_response
    logger.debug(f"{MessageToDict(general_sentiment)}")

    polarity = 'positive' if general_sentiment.sentiment.polarity > 0.0 else 'negative'
    logger.info(f"General tweets sentiment from user_id={user_id} => {polarity}")


def detect_language_command(features_rpc_stub, tweet_id):
    detect_language_response = features_rpc_stub.DetectLanguageFromTweetText(
        DetectLanguageFromTweetTextRequest(tweet_id=tweet_id))

    detect_language = detect_language_response

    logger.debug(f"Detect language on tweet (id={tweet_id}): {MessageToDict(detect_language)}")
