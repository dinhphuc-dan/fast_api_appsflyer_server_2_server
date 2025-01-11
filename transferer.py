import logging
from pathlib import Path
import os
from dotenv import load_dotenv
from utilities import (
    load_logger,
    check_production_args,
    create_postgres_database_and_table,
)

from pydantic import BaseModel
from transferer_kafka_spark_streaming import KafkaSparkStreaming


class AppsflyerS2SMessage(BaseModel):
    idfv: str | None
    device_category: str | None
    store_product_page: str | None
    af_sub1: str | None
    customer_user_id: str | None
    is_lat: str | None
    contributor_2_engagement_type: str | None
    contributor_2_af_prt: str | None
    bundle_id: str | None
    gp_broadcast_referrer: str | None
    contributor_2_touch_time: str | None
    contributor_3_touch_type: str | None
    event_source: str | None
    af_cost_value: str | None
    contributor_1_match_type: str | None
    app_version: str | None
    contributor_3_af_prt: str | None
    custom_data: str | None
    contributor_2_touch_type: str | None
    gp_install_begin: str | None
    city: str | None
    amazon_aid: str | None
    device_model: str | None
    gp_referrer: str | None
    contributor_1_engagement_type: str | None
    af_cost_model: str | None
    af_c_id: str | None
    attributed_touch_time_selected_timezone: str | None
    selected_currency: str | None
    app_name: str | None
    install_time_selected_timezone: str | None
    postal_code: str | None
    wifi: str | None
    install_time: str | None
    engagement_type: str | None
    operator: str | None
    attributed_touch_type: str | None
    af_attribution_lookback: str | None
    campaign_type: str | None
    keyword_match_type: str | None
    af_adset_id: str | None
    device_download_time_selected_timezone: str | None
    contributor_2_media_source: str | None
    conversion_type: str | None
    contributor_2_match_type: str | None
    ad_personalization_enabled: str | None
    api_version: str | None
    attributed_touch_time: str | None
    revenue_in_selected_currency: str | None
    is_retargeting: str | None
    country_code: str | None
    gp_click_time: str | None
    contributor_1_af_prt: str | None
    match_type: str | None
    event_type: str | None
    appsflyer_id: str | None
    dma: str | None
    http_referrer: str | None
    af_sub5: str | None
    af_prt: str | None
    event_revenue_currency: str | None
    store_reinstall: str | None
    install_app_store: str | None
    media_source: str | None
    deeplink_url: str | None
    campaign: str | None
    af_keywords: str | None
    region: str | None
    cost_in_selected_currency: str | None
    event_value: dict | None
    ip: str | None
    oaid: str | None
    event_time: str | None
    is_receipt_validated: str | None
    contributor_1_campaign: str | None
    ad_user_data_enabled: str | None
    af_sub4: str | None
    imei: str | None
    contributor_3_campaign: str | None
    event_revenue_usd: str | None
    af_sub2: str | None
    original_url: str | None
    contributor_2_campaign: str | None
    android_id: str | None
    contributor_3_media_source: str | None
    af_adset: str | None
    contributor_3_engagement_type: str | None
    af_ad: str | None
    state: str | None
    network_account_id: str | None
    device_type: str | None
    idfa: str | None
    retargeting_conversion_type: str | None
    af_channel: str | None
    af_cost_currency: str | None
    contributor_1_media_source: str | None
    custom_dimension: str | None
    keyword_id: str | None
    device_download_time: str | None
    contributor_1_touch_type: str | None
    af_reengagement_window: str | None
    raw_consent_data: str | None
    af_siteid: str | None
    language: str | None
    app_id: str | None
    contributor_1_touch_time: str | None
    event_revenue: str | None
    af_ad_type: str | None
    carrier: str | None
    event_name: str | None
    af_sub_siteid: str | None
    advertising_id: str | None
    gdpr_applies: str | None
    os_version: str | None
    platform: str | None
    af_sub3: str | None
    contributor_3_match_type: str | None
    selected_timezone: str | None
    af_ad_id: str | None
    contributor_3_touch_time: str | None
    user_agent: str | None
    is_primary_attribution: str | None
    sdk_version: str | None
    event_time_selected_timezone: str | None


class Transferer:
    """
    This class aim to check data quality and transfer good data to good_data tables
    While it detects bad data, it will write all unqualified data to a single table name bad_data
    Dad data is the records that are not aligned with the predefined schema
    """

    def __init__() -> None:
        pass


if __name__ == "__main__":
    # setup logging
    logger = load_logger(logger_name="transferer")
    if check_production_args():
        pass
    else:
        load_dotenv(dotenv_path=Path(".env.dev"), override=True, verbose=True)

        stream = KafkaSparkStreaming()
        df = stream.read_from_one_kafka_topic("appsflyer-s2s")
        query = stream.write_to_console(df=df)
        query.awaitTermination()
