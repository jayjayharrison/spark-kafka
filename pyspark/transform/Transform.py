from transform.TransformExtension import TransformExtension
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import get_json_object, col, to_json

from pyspark.sql.types import StructType, ArrayType, MapType


class Transform:

    def __init__(self):
        self.transformExtension = TransformExtension()

    def swith_transform(self,  df: DataFrame, mapping_sheet: str, table: str):
        if 'orbat' in table:
            df = self.orbat_normalize(df=df, mapping_sheet=mapping_sheet)
        else:
            df = self.df_normalize(df=df, mapping_sheet=mapping_sheet)

        return df

    def df_normalize(self, df: DataFrame, mapping_sheet: str):
        """
        flatten json, map columns
        :param df:
        :param mapping_sheet:
        :return:
        """
        nor_df = self.transformExtension.df_normalize(df=df, mapping_sheet=mapping_sheet)
        nor_df_with_sk = self.assign_sk(df=nor_df)
        nor_df_cleaned = self.remove_thousand_delimiter(df=nor_df_with_sk)

        return nor_df_cleaned

    def orbat_normalize(self, df: DataFrame, mapping_sheet: str):
        mapping_dict = self.transformExtension.parse_mapping_sheet(mapping_str=mapping_sheet)
        selectExpr = []
        for field in df.schema.fields:
            name = field.name
            dtype = field.dataType
            if isinstance(dtype, (ArrayType, StructType, MapType)):
                selectExpr.append(to_json(col(name)).alias(name))
            else:
                selectExpr.append(col(name).alias(name))
        df = df.select(selectExpr)
        mapped_df = self.transformExtension.coalesce_map_col(df=df, mapping_dict=mapping_dict)

        return mapped_df


    def json_flatten(self, df: DataFrame):
        """
        flatten json with similar column concatenated

        :param df:
        :return:
        """

        return self.transformExtension.json_flatten(df=df)


    def assign_sk(self, df: DataFrame):
        orderByCol = 'landing_tmst'
        return self.transformExtension.assign_sk(df=df, orderByCol=orderByCol)

    def remove_thousand_delimiter(self, df: DataFrame):
        pattern_replacement = ["(\\d+),(\\d+)", "$1$2"]
        return self.transformExtension.mass_regex_replace(df=df, pattern_replacement=pattern_replacement)



