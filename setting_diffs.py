#!/usr/bin/env python3
import pandas as pd
import dbc.args
from bs4 import BeautifulSoup
from dbc_io.parser import parse_file, Value as ParamValue, RecArray
from typing import List, Tuple

CLUSTER_PREFIX = "/mnt/cluster/glob/simulations/settings/"

parser = dbc.args.ArgumentParser(
    description='Compare settings for param common apt files.')
parser.add_argument(
    '-r', '--reference',
    help='path for the reference file.',
    required=True)
parser.add_argument(
    '-c', '--current',
    help='path for the current file.',
    required=True)
parser.add_argument(
    '-o', '--output',
    help='path to output the html files.',
    required=True)

args = parser.parse_args()


class Setting_diffs:
    def __init__(self):
        self.diffs = self.__compute_diffs()

    def __recursive_compare(
            self,
            d1: ParamValue,
            d2: ParamValue,
            only_d1: List[str],
            only_d2: List[str],
            level='',
            ) -> List[Tuple[str, ParamValue, ParamValue]]:
        """
        Recursive function to get the differences between the two values
        d1 and d2. Function populates the lists keys, d1_values and
        d2 values which we will use to make the first dataframe.
        """
        result: List[Tuple[str, ParamValue, ParamValue]] = []

        if isinstance(d1, RecArray) and isinstance(d2, RecArray):
            d1_dict = {(n or idx + 1): v for idx, (n, v) in enumerate(d1)}
            d2_dict = {(n or idx + 1): v for idx, (n, v) in enumerate(d2)}
            s1 = set(d1_dict.keys())
            s2 = set(d2_dict.keys())
            only_d1.extend(f"{level}.{k}"[1:] for k in s1 - s2)
            only_d2.extend(f"{level}.{k}"[1:] for k in s2 - s1)

            for k in s1.intersection(s2):
                result.extend(self.__recursive_compare(
                    d1_dict[k],
                    d2_dict[k],
                    only_d1=only_d1,
                    only_d2=only_d2,
                    level=f"{level}.{k}"
                ))

        # for the following two settings, sometimes we get '0.00' != '0.000' in
        # the output, and the following code avoids such issues.
        elif (
                d1 != d2 and (
                    level.split('.')[-1] not in [
                        'Min_Deviation_Auction_Increase_Position',
                        'Min_Deviation_Auction_Decrease_Position']
                    or float(d1) != float(d2)
                )
             ):
            result.append((level[1:], d1, d2))
        return result

    def __compute_diffs(self) -> pd.DataFrame:
        d_current = parse_file(args.current)
        d_reference = parse_file(args.reference)
        only_d1: List[str] = []
        only_d2: List[str] = []

        df = pd.DataFrame(
            self.__recursive_compare(d_current, d_reference, only_d1, only_d2)
            + [
                (k, v, '-')
                for k in only_d1
                for v in d_current.get_path(*k.split('.'))
            ] + [
                (k, '-', v)
                for k in only_d2
                for v in d_reference.get_path(*k.split('.'))
            ],
            columns=['Settings', 'Source', 'Reference'])
        df = self.fix_exclusive_table(df).reset_index(drop=True)

        # Remove "strategy_settings" prefixes
        for i in range(len(df)):
            df['Settings'][i] = df['Settings'][i].replace(
                'strategy_settings.', '')

        return df.set_index('Settings')

    def fix_pandas_html(self, html_df):
        style_dict = self.make_styler_dict(html_df)
        html_out = html_df.to_html(formatters=style_dict)
        soup = BeautifulSoup(html_out, 'html.parser')
        soup.table['class'] = "table-fill"
        soup.table['border'] = "0"
        return str(soup)

    def make_styler_dict(self, df):
        d_out = {}
        for col in df.columns:
            try:
                if df[col].abs().mean() < 10.0:
                    d_out[col] = lambda x: '{:,.4f}'.format(x)
                else:
                    d_out[col] = lambda x: '{:,.0f}'.format(x)
            except TypeError:  # --> it's a non-numerical column
                continue
        return d_out

    def fix_exclusive_table(self, df):
        """
        Function to fix the second dataframe where we get duplicated
        differences due to the '1' and '2' values in the keys of the nested
        dictionaries.
        """
        for i in range(len(df)):
            settings_l = df['Settings'][i].split('.')

            if '1' in settings_l or '2' in settings_l:
                try:
                    settings_l.remove('1')
                except:
                    settings_l.remove('2')

            df['Settings'][i] = '.'.join(settings_l)

        return df.drop_duplicates(subset=['Settings'])

    def output_html_file(self):
        try:
            with open(CLUSTER_PREFIX +
                      'cluster_weekly_simulations/assets/stylesheet.css',
                      'r') as fo:
                stylesheet = fo.read()
        except FileNotFoundError:
            stylesheet = ''

        with open(args.output, "w") as f:
            f.write(
                stylesheet + '\n'
                + '<body>' + '\n'
                + self.fix_pandas_html(self.diffs)
                + '\n' + '</body>'
            )


def main():
    pd.set_option('display.max_colwidth', 500)

    differences = Setting_diffs()

    if not differences.diffs.empty:
        print()
        print(f'Differences between {args.current} and {args.reference}:\n')
        print(differences.diffs.to_string())
        differences.output_html_file()


if __name__ == '__main__':
    main()
