from pyspark.sql.types import StructType, ArrayType
from pyspark.sql.functions import col, lit, coalesce, trim, when, row_number, concat, lpad, regexp_replace, concat_ws
from pyspark.sql.dataframe import DataFrame
import re
from utils.S3Util import S3BucketUtil
from datetime import datetime
from pyspark.sql import Window
import pandas


class TransformExtension:
    def __init__(self):
        None


    def df_normalize(self, df: DataFrame, mapping_sheet: str):
        mapping_dict = self.parse_mapping_sheet(mapping_str=mapping_sheet)

        flatten_df = self.json_flatten(df=df)
        all_src_cols = self.get_all_src_col_from_mapping(mapping_dict=mapping_dict)

        df_filled = self.df_add_cols_literal(df=flatten_df, cols=all_src_cols)

        mapped_df = self.coalesce_map_col(df=df_filled, mapping_dict=mapping_dict)

        return mapped_df

    def coalesce_map_col(self, df: DataFrame, mapping_dict: list):
        select_expr = self.build_mapping_exprs(mapping_dict=mapping_dict)
        mapped_df = df.select(select_expr)
        return mapped_df

    def build_mapping_exprs(self, mapping_dict: list):
        exprs = []
        for dic_ in mapping_dict:
            tgt_col = dic_['target_col']
            src_cols = dic_['source_cols']
            expr = coalesce(*[when((trim(col(c)) != "") & (col(c).isNotNull()), col(c)).otherwise(None) for c in src_cols]).alias(tgt_col)
            exprs.append(expr)

        return exprs


    def assign_sk(self, df: DataFrame, orderByCol: str):
        now = datetime.now()  # current date and time
        fmt = '%y%m%d%H'
        yymmddhh = now.strftime(fmt)
        df_with_row_num = df.withColumn("row_num", row_number().over(Window.orderBy(col(orderByCol))))
        sk_df = df_with_row_num.select(concat(lit(yymmddhh), lpad(col("row_num"), 5, "0")).cast("long").alias("sys_sk"), col("*")).drop(col("row_num"))
        return sk_df


    def mass_regex_replace(self,  df: DataFrame, pattern_replacement: list):
        pattern = pattern_replacement[0]
        replacement = pattern_replacement[1]
        cols = df.schema.fieldNames()
        selectExpr = [regexp_replace(col(c), pattern, replacement).alias(c) for c in cols]
        return df.select(selectExpr)

    def df_add_cols_literal(self, df: DataFrame, cols: list):
        existing_cols = df.columns
        missing_cols = [col for col in cols if col not in existing_cols]
        missing_cols = list(set(missing_cols)) # remove duplicate
        missing_cols_expr = [lit(None).alias(col) for col in missing_cols]
        all_expr = ["*"] + missing_cols_expr
        return df.select(all_expr)


    def get_all_src_col_from_mapping(self, mapping_dict: list):
        src_cols = []
        for dic_ in mapping_dict:
            src_cols.extend(dic_['source_cols'])
        src_cols = list(set(src_cols))
        return src_cols


    def parse_mapping_sheet(self, mapping_str: str):
        """
        return [{'target_col': tgt_col, 'source_cols': [src_cols]} , ...]

        :param mapping_str:
        :return:
        """
        lines = mapping_str.split('\n')

        def to_dict_(line: str):
            cols = line.split('|')
            tgt_col = cols[0].strip()
            src_cols = re.sub(r'\s+|,$', '', cols[1].strip()).split(',')
            return {'target_col': tgt_col, 'source_cols': src_cols}
        lines = list(filter(lambda l: l.strip() != '', lines))  # filter out empty line
        mapping_dict = list(map(lambda r: to_dict_(r), lines))
        return mapping_dict


    def json_flatten(self, df: DataFrame):
        schema = df.schema
        flatten_df = df.select(self.on_flatten(schema))

        flatten_df_agg = self.concat_similar_columns(df=flatten_df)

        return flatten_df_agg


    def concat_similar_columns(self, df: DataFrame):
        """
        replace - to _ in column name,
        concat all similar column with ' | '

        :param df:
        :return:
        """
        fields = df.schema.fieldNames()
        pandas_df = pandas.DataFrame(data={'field': fields})
        pandas_df['groupfiled'] = pandas_df.apply(func=lambda r: r['field'].replace('-', '_'),  axis=1)
        group_ser = pandas_df.groupby('groupfiled', as_index=False)['field'].apply(list)
        group_fileds = group_ser.to_list()
        SEP = ' | '
        selectExpr = [concat_ws(SEP, *[col(f) for f in fields]).alias(fields[0]) for fields in group_fileds]
        return df.select(selectExpr)

    @staticmethod
    def normalise_field(raw):
        return raw.strip().lower() \
                .replace('`', '') \
                .replace('.', '-')


    def on_flatten(self, schema, prefix=None):
        fields = []
        for field in schema.fields:
            name = "%s.`%s`" % (prefix, field.name) if prefix else "`%s`" % field.name
            dtype = field.dataType
            if isinstance(dtype, ArrayType):
                dtype = dtype.elementType
            if isinstance(dtype, StructType):
                fields += self.on_flatten(dtype, prefix=name)
            else:
                fields.append(col(name).alias(self.normalise_field(name)))

        return fields



