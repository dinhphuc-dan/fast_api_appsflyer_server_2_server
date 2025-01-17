import logging
import logging.config
import yaml
import os

import argparse
from enum import Enum
from sqlmodel import Field, SQLModel
import datetime


def check_production_args():
    # basic setup for argument throug command line for local testing and development
    _parser = argparse.ArgumentParser()
    _parser.add_argument("--production")
    _args = _parser.parse_args()
    if _args.production == "true":
        return True
    else:
        return False


def load_logger(logger_name) -> logging.Logger:
    with open("log_conf.yaml", "r") as f:
        logging.config.dictConfig(yaml.safe_load(f))
    return logging.getLogger(logger_name)


class ValidPath(str, Enum):
    """
    Class affects valid HTTP path from collectors
    """

    appsflyer_s2s = "appsflyer-s2s"
    google_play_store_transaction = "google-play-store-transaction"
    apple_store_transaction = "apple-store-transaction"


class BadDataTable(SQLModel, table=True):
    row_id: int | None = Field(default=None, primary_key=True)
    json_data_string: str
    error_reason: str
    path: str
    event_time: datetime.date


# class for validate event schema and create table if not exists in database
class AppsflyerS2S(SQLModel, table=True):
    row_id: int | None = Field(default=None, primary_key=True)
    appsflyer_id: str
    bundle_id: str
    app_id: str
    event_name: str
    event_time: str
    af_ad_id: str | None = None
    idfv: str | None = None
    device_category: str | None = None
    store_product_page: str | None = None
    af_sub1: str | None = None
    customer_user_id: str | None = None
    is_lat: str | None = None
    contributor_2_engagement_type: str | None = None
    contributor_2_af_prt: str | None = None
    gp_broadcast_referrer: str | None = None
    contributor_2_touch_time: str | None = None
    contributor_3_touch_type: str | None = None
    event_source: str | None = None
    af_cost_value: str | None = None
    contributor_1_match_type: str | None = None
    app_version: str | None = None
    contributor_3_af_prt: str | None = None
    custom_data: str | None = None
    contributor_2_touch_type: str | None = None
    gp_install_begin: str | None = None
    city: str | None = None
    amazon_aid: str | None = None
    device_model: str | None = None
    gp_referrer: str | None = None
    contributor_1_engagement_type: str | None = None
    af_cost_model: str | None = None
    af_c_id: str | None = None
    attributed_touch_time_selected_timezone: str | None = None
    selected_currency: str | None = None
    app_name: str | None = None
    install_time_selected_timezone: str | None = None
    postal_code: str | None = None
    wifi: str | None = None
    install_time: str | None = None
    engagement_type: str | None = None
    operator: str | None = None
    attributed_touch_type: str | None = None
    af_attribution_lookback: str | None = None
    campaign_type: str | None = None
    keyword_match_type: str | None = None
    af_adset_id: str | None = None
    device_download_time_selected_timezone: str | None = None
    contributor_2_media_source: str | None = None
    conversion_type: str | None = None
    contributor_2_match_type: str | None = None
    ad_personalization_enabled: str | None = None
    api_version: str | None = None
    attributed_touch_time: str | None = None
    revenue_in_selected_currency: str | None = None
    is_retargeting: str | None = None
    country_code: str | None = None
    gp_click_time: str | None = None
    contributor_1_af_prt: str | None = None
    match_type: str | None = None
    event_type: str | None = None
    dma: str | None = None
    http_referrer: str | None = None
    af_sub5: str | None = None
    af_prt: str | None = None
    event_revenue_currency: str | None = None
    store_reinstall: str | None = None
    install_app_store: str | None = None
    media_source: str | None = None
    deeplink_url: str | None = None
    campaign: str | None = None
    af_keywords: str | None = None
    region: str | None = None
    cost_in_selected_currency: str | None = None
    event_value: str | None = None
    ip: str | None = None
    oaid: str | None = None
    event_time_selected_timezone: str | None = None
    is_receipt_validated: str | None = None
    contributor_1_campaign: str | None = None
    ad_user_data_enabled: str | None = None
    af_sub4: str | None = None
    imei: str | None = None
    contributor_3_campaign: str | None = None
    event_revenue_usd: str | None = None
    af_sub2: str | None = None
    original_url: str | None = None
    contributor_2_campaign: str | None = None
    android_id: str | None = None
    contributor_3_media_source: str | None = None
    af_adset: str | None = None
    contributor_3_engagement_type: str | None = None
    af_ad: str | None = None
    state: str | None = None
    network_account_id: str | None = None
    device_type: str | None = None
    idfa: str | None = None
    retargeting_conversion_type: str | None = None
    af_channel: str | None = None
    af_cost_currency: str | None = None
    contributor_1_media_source: str | None = None
    custom_dimension: str | None = None
    keyword_id: str | None = None
    device_download_time: str | None = None
    contributor_1_touch_type: str | None = None
    af_reengagement_window: str | None = None
    raw_consent_data: str | None = None
    af_siteid: str | None = None
    language: str | None = None
    contributor_1_touch_time: str | None = None
    event_revenue: str | None = None
    af_ad_type: str | None = None
    carrier: str | None = None
    af_sub_siteid: str | None = None
    advertising_id: str | None = None
    gdpr_applies: str | None = None
    os_version: str | None = None
    platform: str | None = None
    af_sub3: str | None = None
    contributor_3_match_type: str | None = None
    selected_timezone: str | None = None
    contributor_3_touch_time: str | None = None
    user_agent: str | None = None
    is_primary_attribution: str | None = None
    sdk_version: str | None = None
