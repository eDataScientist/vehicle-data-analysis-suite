import pandas as pd
import io
from typing import Dict, List, Tuple, Optional, Union


class DiscrepancyAnalyzer:
    """Vehicle valuation discrepancy analyzer"""

    def __init__(self):
        self.primary_data = None
        self.join_data = None
        self.primary_sheets = None
        self.join_sheets = None

    def load_primary_data(self, file_data: Union[io.BytesIO, str], file_type: str = 'xlsx') -> List[str]:
        """
        Load primary data file and return available sheets

        Args:
            file_data: File data (BytesIO for uploaded files)
            file_type: 'xlsx', 'xls', or 'csv'

        Returns:
            List of sheet names (for Excel) or ['Data'] for CSV
        """
        if file_type in ['xlsx', 'xls']:
            xls = pd.ExcelFile(file_data)
            self.primary_sheets = xls.sheet_names
            return self.primary_sheets
        else:
            # CSV file
            self.primary_data = pd.read_csv(file_data)
            self.primary_sheets = ['Data']
            return ['Data']

    def select_primary_sheet(self, sheet_name: str, file_data: Union[io.BytesIO, str], file_type: str = 'xlsx') -> pd.DataFrame:
        """
        Select and load a specific sheet from primary data

        Args:
            sheet_name: Name of the sheet to load
            file_data: File data
            file_type: File type

        Returns:
            DataFrame with the selected sheet data
        """
        if file_type in ['xlsx', 'xls']:
            xls = pd.ExcelFile(file_data)
            self.primary_data = xls.parse(sheet_name)
        else:
            # Already loaded for CSV
            pass

        return self.primary_data

    def load_join_data(self, file_data: Union[io.BytesIO, str], file_type: str = 'xlsx') -> List[str]:
        """
        Load join data file and return available sheets

        Args:
            file_data: File data (BytesIO for uploaded files)
            file_type: 'xlsx', 'xls', or 'csv'

        Returns:
            List of sheet names (for Excel) or ['Data'] for CSV
        """
        if file_type in ['xlsx', 'xls']:
            xls = pd.ExcelFile(file_data)
            self.join_sheets = xls.sheet_names
            return self.join_sheets
        else:
            # CSV file
            self.join_data = pd.read_csv(file_data)
            self.join_sheets = ['Data']
            return ['Data']

    def select_join_sheet(self, sheet_name: str, file_data: Union[io.BytesIO, str], file_type: str = 'xlsx') -> pd.DataFrame:
        """
        Select and load a specific sheet from join data

        Args:
            sheet_name: Name of the sheet to load
            file_data: File data
            file_type: File type

        Returns:
            DataFrame with the selected sheet data
        """
        if file_type in ['xlsx', 'xls']:
            xls = pd.ExcelFile(file_data)
            self.join_data = xls.parse(sheet_name)
        else:
            # Already loaded for CSV
            pass

        return self.join_data

    def get_primary_columns(self) -> List[str]:
        """Get list of available columns from primary data"""
        if self.primary_data is not None:
            return list(self.primary_data.columns)
        return []

    def get_join_columns(self) -> List[str]:
        """Get list of available columns from join data"""
        if self.join_data is not None:
            return list(self.join_data.columns)
        return []

    def generate_report(self,
                        vin_col: str,
                        no_calc: bool = False,
                        threshold: float = 15.0,
                        val_col1: str = None,
                        val_col2: str = None,
                        percent_diff_col: str = None,
                        vin_join_col: str = None,
                        make_col: str = None,
                        model_col: str = None,
                        year_col: str = None,
                        price_col: str = None,
                        single_file_mode: bool = False) -> io.BytesIO:
        """
        Generate discrepancy report

        Args:
            vin_col: VIN column name in primary data
            no_calc: Whether to use precomputed percentage difference
            threshold: Threshold percentage for discrepancy detection
            val_col1: First value column (if calculating)
            val_col2: Second value column (if calculating)
            percent_diff_col: Precomputed percentage difference column (if no_calc)
            vin_join_col: VIN column name in join data (not used in single file mode)
            make_col: Make column name (in join data for two file mode, in primary data for single file mode)
            model_col: Model column name (in join data for two file mode, in primary data for single file mode)
            year_col: Year column name (in join data for two file mode, in primary data for single file mode)
            price_col: Price column for price range analysis
            single_file_mode: Whether to use single file mode (all columns from primary data)

        Returns:
            BytesIO object containing the Excel report
        """
        if self.primary_data is None:
            raise ValueError("Primary data must be loaded")

        if not single_file_mode and self.join_data is None:
            raise ValueError(
                "Join data must be loaded when not in single file mode")

        # Handle single file mode vs two file mode
        if single_file_mode:
            # All columns come from primary data, no join needed
            df = self.primary_data.rename(columns={
                vin_col: 'VIN',
                make_col: 'Make',
                model_col: 'Model',
                year_col: 'ModelYear'
            })
        else:
            # Traditional two file mode with join operation
            df_val = self.primary_data.rename(columns={vin_col: 'VIN'})
            df_join = self.join_data.rename(columns={vin_join_col: 'VIN'})

            # Merge on VIN
            df = df_val.merge(df_join[['VIN', make_col, model_col, year_col]],
                              on='VIN', how='left').rename(
                columns={make_col: 'Make', model_col: 'Model', year_col: 'ModelYear'})

        # Compute percentage difference if not in no_calc mode
        if not no_calc:
            # Calculate signed percentage difference
            df['Percent_Diff'] = (
                df[val_col1] - df[val_col2]) / df[val_col2] * 100
        else:
            # Use precomputed column
            df['Percent_Diff'] = df[percent_diff_col]

        # Fixed flags - properly detect under/over valuation
        df['Within'] = df['Percent_Diff'].abs() <= threshold
        df['Above'] = df['Percent_Diff'] > threshold  # Overvalued
        df['Below'] = df['Percent_Diff'] < -threshold  # Undervalued

        total = len(df)
        within = df['Within'].sum()
        above = df['Above'].sum()
        below = df['Below'].sum()

        # Build tables
        exec_df = pd.DataFrame({
            'Metric': ['Within tolerance', 'Above threshold', 'Below threshold'],
            'Count':  [within, above, below],
            'Percentage': [within/total*100, above/total*100, below/total*100]
        })

        grp = df.groupby('Make').agg(Total=('VIN', 'count'),
                                     Affected=('Above', 'sum'))
        grp['Discrepancy Rate'] = grp['Affected']/grp['Total']*100
        avg_diff = df[df['Above']].groupby(
            'Make')['Percent_Diff'].mean().rename('Average Difference')
        brand_perf = grp.join(avg_diff).reset_index()
        brand_perf = brand_perf[brand_perf['Total'] >= 15].sort_values(
            'Discrepancy Rate', ascending=False).head(12)

        hv = brand_perf.sort_values('Total', ascending=False).head(8)

        bins = [0, 2014, 2019, 2022, 9999]
        labels = ['Pre-2015', '2015-2019', '2020-2022', '2023+']
        df['YearGroup'] = pd.cut(df['ModelYear'], bins=bins, labels=labels)
        my = df.groupby('YearGroup').agg(Total=('VIN', 'count'),
                                         Affected=('Above', 'sum'),
                                         **{'Average Difference': ('Percent_Diff', 'mean')})
        my['Discrepancy Rate'] = my['Affected']/my['Total']*100
        my = my.reset_index()[['YearGroup', 'Discrepancy Rate',
                              'Affected', 'Total', 'Average Difference']]

        # Price range analysis
        price_column = price_col if price_col else (
            val_col2 if not no_calc else 'Price')
        pr_bins = [0, 5000, 15000, 30000, 50000, 1e12]
        pr_labels = ['Under $5K', '$5K-$15K',
                     '$15K-$30K', '$30K-$50K', 'Above $50K']
        df['PriceRange'] = pd.cut(
            df[price_column], bins=pr_bins, labels=pr_labels)
        pr = df.groupby('PriceRange').agg(Total=('VIN', 'count'),
                                          Affected=('Above', 'sum'),
                                          **{'Average Difference': ('Percent_Diff', 'mean')})
        pr['Discrepancy Rate'] = pr['Affected']/pr['Total']*100
        pr = pr.reset_index()[['PriceRange', 'Discrepancy Rate',
                              'Affected', 'Total', 'Average Difference']]

        vd = df['PriceRange'].value_counts().reindex(
            pr_labels).rename('Vehicle Count').reset_index()
        vd.columns = ['Price Range', 'Vehicle Count']
        vd['Percentage of Total'] = vd['Vehicle Count']/total*100

        invalid = df['VIN'].isna().sum()
        dq = pd.DataFrame({
            'Metric': ['Total records processed', 'Data completeness', 'Invalid/excluded records'],
            'Value': [total, '100%', invalid]
        })

        # Write to Excel
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            start = 0
            exec_df.to_excel(writer, sheet_name='Report',
                             index=False, startrow=start)
            start += len(exec_df) + 2
            brand_perf.to_excel(writer, sheet_name='Report',
                                index=False, startrow=start)
            start += len(brand_perf) + 2
            hv.to_excel(writer, sheet_name='Report',
                        index=False, startrow=start)
            start += len(hv) + 2
            my.to_excel(writer, sheet_name='Report',
                        index=False, startrow=start)
            start += len(my) + 2
            pr.to_excel(writer, sheet_name='Report',
                        index=False, startrow=start)
            start += len(pr) + 2
            vd.to_excel(writer, sheet_name='Report',
                        index=False, startrow=start)
            start += len(vd) + 2
            dq.to_excel(writer, sheet_name='Report',
                        index=False, startrow=start)

            # Also save the detailed data
            df.to_excel(writer, sheet_name='Detailed_Data', index=False)

        output.seek(0)
        return output
