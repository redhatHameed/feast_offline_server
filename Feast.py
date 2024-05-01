from feast import FeatureStore
from datetime import datetime
from feast_offline_server.config import Config


class OfflineFeatureStore:
    def __init__(self):
        self.config = Config()

    def get_historical_features(self, entity_df, features):
        feature_store_yaml = self.config.get_feature_store_yaml()
        store = FeatureStore(feature_store_yaml)
        training_df = store.get_historical_features(
            entity_df=entity_df,
            features=features
        )

        return training_df

    def set_materialize_incremental(self):
        feature_store_yaml = self.config.get_feature_store_yaml()

        store = FeatureStore(feature_store_yaml)
        end_date = datetime.now()
        store.materialize_incremental(end_date=end_date)
