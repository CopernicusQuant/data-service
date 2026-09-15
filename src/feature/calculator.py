import pandas as pd
from joblib import Parallel, delayed

from .calculator_utils import (
    combine_stock_basics,
    compute_activity_features,
    compute_bollinger_bands,
    compute_cci,
    compute_dema,
    compute_kdj,
    compute_macd,
    compute_obv,
    compute_price_momentum,
    compute_rsi,
    compute_volatility,
    compute_volume_momentum,
)


class FeatureCalculator:
    def compute_all_stock_features(
        self, combined_stock_df: pd.DataFrame, combined_info_df: pd.DataFrame
    ) -> pd.DataFrame:
        # we should ensure the input stock df has two level of indices
        assert combined_stock_df.index.nlevels == 2, (
            "Input must have two indices (ts_code, trade_date)"
        )
        grouped = combined_stock_df.groupby(level="ts_code")

        def _compute_per_stock(group: pd.DataFrame) -> pd.DataFrame:
            group = group.reset_index(level="ts_code")
            ts_code = group["ts_code"].iloc[0]
            stock_info = combined_info_df[combined_info_df["ts_code"] == ts_code]
            result = self.compute_stock_features(
                stock_data_df=group, stock_info_df=stock_info
            )
            result = result.copy()
            result = result.reset_index()
            result.set_index(["ts_code", "trade_date"], inplace=True)
            return result

        results = Parallel(n_jobs=4, verbose=1, backend="threading")(
            delayed(_compute_per_stock)(group) for _, group in grouped
        )
        result = pd.concat([r for r in results if r is not None], axis=0)
        return result

    def compute_stock_features(
        self, stock_data_df: pd.DataFrame, stock_info_df
    ) -> pd.DataFrame:
        new_features = [
            combine_stock_basics(
                stock_data_df=stock_data_df, stock_info_df=stock_info_df
            ),
            compute_bollinger_bands(df=stock_data_df),
            compute_activity_features(df=stock_data_df),
            compute_cci(df=stock_data_df),
            compute_dema(df=stock_data_df),
            compute_kdj(df=stock_data_df),
            compute_macd(df=stock_data_df),
            compute_obv(df=stock_data_df),
            compute_price_momentum(df=stock_data_df),
            compute_rsi(df=stock_data_df),
            compute_volatility(df=stock_data_df),
            compute_volume_momentum(df=stock_data_df),
        ]
        result = pd.concat(new_features, axis=1)
        return result.sort_index()
