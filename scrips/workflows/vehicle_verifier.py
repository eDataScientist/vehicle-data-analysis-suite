import pandas as pd
import io
import asyncio
from typing import Dict, List, Tuple, Optional, Union
import openpyxl
from openpyxl.utils import get_column_letter
from openpyxl.formatting.rule import FormulaRule
from openpyxl.styles import PatternFill

# Import translation service
try:
    from ..services.translation_service import ArabicTranslationService
    TRANSLATION_AVAILABLE = True
except ImportError:
    TRANSLATION_AVAILABLE = False


class VehicleDataVerifier:
    """Vehicle data verification with optional translation capabilities"""

    def __init__(self):
        self.reference_data = None
        self.logs_data = None
        self.reference_sheets = None
        self.logs_sheets = None
        self.merged_data = None
        # Initialize translation service if available
        self.translation_service = ArabicTranslationService() if TRANSLATION_AVAILABLE else None

    def load_reference_data(self, file_data: Union[io.BytesIO, str], file_type: str = 'xlsx') -> List[str]:
        """Load reference data file and return available sheets"""
        if file_type in ['xlsx', 'xls']:
            xls = pd.ExcelFile(file_data)
            self.reference_sheets = xls.sheet_names
            return self.reference_sheets
        else:
            self.reference_data = pd.read_csv(file_data, encoding='utf-8')
            self.reference_sheets = ['Data']
            return ['Data']

    def select_reference_sheet(self, sheet_name: str, file_data: Union[io.BytesIO, str], file_type: str = 'xlsx') -> pd.DataFrame:
        """Select and load a specific sheet from reference data"""
        if file_type in ['xlsx', 'xls']:
            xls = pd.ExcelFile(file_data)
            self.reference_data = xls.parse(sheet_name)
        return self.reference_data

    def load_logs_data(self, file_data: Union[io.BytesIO, str], file_type: str = 'xlsx') -> List[str]:
        """Load logs data file and return available sheets"""
        if file_type in ['xlsx', 'xls']:
            xls = pd.ExcelFile(file_data)
            self.logs_sheets = xls.sheet_names
            return self.logs_sheets
        else:
            self.logs_data = pd.read_csv(file_data, encoding='utf-8')
            self.logs_sheets = ['Data']
            return ['Data']

    def select_logs_sheet(self, sheet_name: str, file_data: Union[io.BytesIO, str], file_type: str = 'xlsx') -> pd.DataFrame:
        """Select and load a specific sheet from logs data"""
        if file_type in ['xlsx', 'xls']:
            xls = pd.ExcelFile(file_data)
            self.logs_data = xls.parse(sheet_name)
        return self.logs_data

    def get_reference_columns(self) -> List[str]:
        """Get list of available columns from reference data"""
        if self.reference_data is not None:
            return list(self.reference_data.columns)
        return []

    def get_logs_columns(self) -> List[str]:
        """Get list of available columns from logs data"""
        if self.logs_data is not None:
            return list(self.logs_data.columns)
        return []

    def prepare_reference_data(self, chassis_col: str, make_col: str, model_col: str, year_col: str) -> pd.DataFrame:
        """Prepare reference data with column mappings"""
        if self.reference_data is None:
            raise ValueError("Reference data not loaded")
        
        # Check if columns exist in the data
        missing_cols = []
        for col_name, col_val in [('chassis_col', chassis_col), ('make_col', make_col), 
                                 ('model_col', model_col), ('year_col', year_col)]:
            if col_val not in self.reference_data.columns:
                missing_cols.append(f"{col_name}: '{col_val}'")
        
        if missing_cols:
            available_cols = list(self.reference_data.columns)
            raise ValueError(f"Missing columns in reference data: {missing_cols}. Available columns: {available_cols}")

        # Apply mappings and rename columns
        mapped_data = {
            'chassis_no': self.reference_data[chassis_col],
            'Make_ext': self.reference_data[make_col],
            'Model_ext': self.reference_data[model_col],
            'ModelYear_ext': self.reference_data[year_col]
        }

        self.reference_data = pd.DataFrame(mapped_data)
        return self.reference_data

    def prepare_logs_data(self, vin_col: str, make_col: str, model_col: str, year_col: str, spec_status_col: str) -> pd.DataFrame:
        """Prepare logs data with column mappings"""
        if self.logs_data is None:
            raise ValueError("Logs data not loaded")
        
        # Check if columns exist in the data
        missing_cols = []
        for col_name, col_val in [('vin_col', vin_col), ('make_col', make_col), 
                                 ('model_col', model_col), ('year_col', year_col), 
                                 ('spec_status_col', spec_status_col)]:
            if col_val not in self.logs_data.columns:
                missing_cols.append(f"{col_name}: '{col_val}'")
        
        if missing_cols:
            available_cols = list(self.logs_data.columns)
            raise ValueError(f"Missing columns in logs data: {missing_cols}. Available columns: {available_cols}")

        # Apply mappings
        mapped_data = {
            'VIN': self.logs_data[vin_col],
            'Make': self.logs_data[make_col],
            'Model': self.logs_data[model_col],
            'ModelYear': self.logs_data[year_col],
            'Specification Status': self.logs_data[spec_status_col]
        }

        self.logs_data = pd.DataFrame(mapped_data)
        return self.logs_data


    async def perform_translation(self, api_key: str, progress_callback=None) -> Dict[str, str]:
        """Perform Arabic to English translation on brand and model names"""
        if not TRANSLATION_AVAILABLE or not self.translation_service:
            return {"error": "Translation not available. Install openai library."}

        if self.reference_data is None:
            return {"error": "Reference data not loaded"}

        try:
            # Use the translation service to translate the DataFrame columns
            self.reference_data = await self.translation_service.translate_dataframe_columns(
                self.reference_data,
                ['Make_ext', 'Model_ext'],
                api_key,
                progress_callback
            )
            
            # Get statistics for response
            brand_strings = self.reference_data['Make_ext'].astype(str).tolist()
            model_strings = self.reference_data['Model_ext'].astype(str).tolist()
            
            ar_brand_count = len([s for s in brand_strings if self.translation_service.detect_arabic_text(s)])
            ar_model_count = len([s for s in model_strings if self.translation_service.detect_arabic_text(s)])
            
            if ar_brand_count == 0 and ar_model_count == 0:
                return {"status": "Translation completed - no Arabic text remaining"}
            
            return {
                "status": "Translation completed",
                "brand_translations": ar_brand_count,
                "model_translations": ar_model_count
            }
        except Exception as e:
            return {"error": f"Translation failed: {str(e)}"}

    def perform_verification(self, chassis_col: str, make_ext_col: str, model_ext_col: str, year_ext_col: str, vin_col: str, make_col: str, model_col: str, year_col: str, spec_status_col: str) -> Dict[str, Union[int, float, pd.DataFrame]]:
        """Perform verification analysis between reference and logs data"""
        if self.reference_data is None or self.logs_data is None:
            raise ValueError("Both reference and logs data must be loaded")
        
        # First prepare the data with the provided column mappings
        self.prepare_reference_data(chassis_col, make_ext_col, model_ext_col, year_ext_col)
        self.prepare_logs_data(vin_col, make_col, model_col, year_col, spec_status_col)
        
        # Now use the standardized column names for the working copies
        ref_working = self.reference_data.copy()
        logs_working = self.logs_data.copy()

        # Merge datasets using standardized column names
        self.merged_data = logs_working.merge(
            ref_working,
            left_on='VIN',
            right_on='chassis_no',
            how='left'
        )

        # Calculate match statistics for reporting (but don't add to merged_data)
        make_matches_calc = (
            self.merged_data['Make'].str.lower() ==
            self.merged_data['Make_ext'].str.lower()
        )

        model_matches_calc = (
            self.merged_data['Model'].str.lower() ==
            self.merged_data['Model_ext'].str.lower()
        )

        year_matches_calc = (
            self.merged_data['ModelYear'].astype(str) ==
            self.merged_data['ModelYear_ext'].astype(str)
        )

        # Calculate match statistics
        total_records = len(self.merged_data)
        make_matches = make_matches_calc.sum()
        model_matches = model_matches_calc.sum()
        year_matches = year_matches_calc.sum()

        # Get mismatches
        mismatches = self.merged_data[
            ~make_matches_calc |
            ~model_matches_calc |
            ~year_matches_calc
        ]

        return {
            'total_records': total_records,
            'make_matches': make_matches,
            'model_matches': model_matches,
            'year_matches': year_matches,
            'make_match_percentage': make_matches / total_records * 100 if total_records > 0 else 0,
            'model_match_percentage': model_matches / total_records * 100 if total_records > 0 else 0,
            'year_match_percentage': year_matches / total_records * 100 if total_records > 0 else 0,
            'mismatches': mismatches,
            'mismatches_count': len(mismatches)
        }

    def save_results(self, include_mask_in_main: bool = True) -> io.BytesIO:
        """Save analysis results to Excel file with dynamic conditional formatting"""
        if self.merged_data is None:
            raise ValueError(
                "No verification data available. Run verification first.")

        # Calculate match statistics
        make_matches_calc = (
            self.merged_data['Make'].str.lower() == 
            self.merged_data['Make_ext'].str.lower()
        )
        model_matches_calc = (
            self.merged_data['Model'].str.lower() == 
            self.merged_data['Model_ext'].str.lower()
        )
        year_matches_calc = (
            self.merged_data['ModelYear'].astype(str) == 
            self.merged_data['ModelYear_ext'].astype(str)
        )

        # Prepare data for saving
        data_to_save = self.merged_data.copy()
        
        # Add match columns to main sheet if requested
        if include_mask_in_main:
            data_to_save['Make Match'] = make_matches_calc
            data_to_save['Model Match'] = model_matches_calc
            data_to_save['Year Match'] = year_matches_calc

        output = io.BytesIO()

        # Save the main data
        data_to_save.to_excel(output, sheet_name='Analysis', index=False)
        output.seek(0)

        # Load workbook for advanced formatting
        wb = openpyxl.load_workbook(output)
        ws = wb['Analysis']
        
        # Replace boolean values with Excel formulas if match columns are included
        if include_mask_in_main:
            # Map headers to column letters
            headers = {cell.value: cell.column for cell in ws[1]}
            
            # Get column letters for data columns
            make_col = get_column_letter(headers.get('Make', 1))
            make_ext_col = get_column_letter(headers.get('Make_ext', 1))
            model_col = get_column_letter(headers.get('Model', 1))
            model_ext_col = get_column_letter(headers.get('Model_ext', 1))
            year_col = get_column_letter(headers.get('ModelYear', 1))
            year_ext_col = get_column_letter(headers.get('ModelYear_ext', 1))
            
            max_row = ws.max_row
            
            # Replace boolean values with Excel formulas in match columns
            if 'Make Match' in headers:
                make_match_col = get_column_letter(headers['Make Match'])
                for r in range(2, max_row + 1):
                    ws[f"{make_match_col}{r}"] = f"=UPPER({make_col}{r})=UPPER({make_ext_col}{r})"
            
            if 'Model Match' in headers:
                model_match_col = get_column_letter(headers['Model Match'])
                for r in range(2, max_row + 1):
                    ws[f"{model_match_col}{r}"] = f"=UPPER({model_col}{r})=UPPER({model_ext_col}{r})"
            
            if 'Year Match' in headers:
                year_match_col = get_column_letter(headers['Year Match'])
                for r in range(2, max_row + 1):
                    ws[f"{year_match_col}{r}"] = f"=TEXT({year_col}{r},\"0\")=TEXT({year_ext_col}{r},\"0\")"
        
        # Create Summary sheet
        summary_data = {
            'Metric': ['Total Records', 'Make Matches', 'Model Matches', 'Year Matches'],
            'Count': [
                len(self.merged_data),
                make_matches_calc.sum(),
                model_matches_calc.sum(),
                year_matches_calc.sum()
            ],
            'Percentage': [
                100.0,
                make_matches_calc.sum() / len(self.merged_data) * 100,
                model_matches_calc.sum() / len(self.merged_data) * 100,
                year_matches_calc.sum() / len(self.merged_data) * 100
            ]
        }
        summary_df = pd.DataFrame(summary_data)
        
        # Add summary sheet
        summary_ws = wb.create_sheet('Summary')
        for r_idx, row in enumerate(summary_df.values, 1):
            for c_idx, value in enumerate(row, 1):
                summary_ws.cell(row=r_idx, column=c_idx, value=value)
        
        # Add headers to summary
        for c_idx, header in enumerate(summary_df.columns, 1):
            summary_ws.cell(row=1, column=c_idx, value=header)

        # Apply dynamic conditional formatting to Analysis sheet
        self._apply_dynamic_conditional_formatting(wb, ws, include_mask_in_main)

        # Save to BytesIO
        output = io.BytesIO()
        wb.save(output)
        output.seek(0)
        return output

    def _apply_dynamic_conditional_formatting(self, workbook, worksheet, include_mask_in_main: bool = True) -> None:
        """Apply dynamic conditional formatting using Excel formulas that compare columns directly"""
        max_row = worksheet.max_row
        
        # Map headers to column letters
        headers = {cell.value: cell.column for cell in worksheet[1]}
        
        # Define colors
        mint_green = PatternFill(start_color='C6EFCE', end_color='C6EFCE', fill_type='solid')
        coral_pink = PatternFill(start_color='F8D7DA', end_color='F8D7DA', fill_type='solid')
        
        # Get column letters for data columns
        make_col = get_column_letter(headers.get('Make', 1))
        make_ext_col = get_column_letter(headers.get('Make_ext', 1))
        model_col = get_column_letter(headers.get('Model', 1))
        model_ext_col = get_column_letter(headers.get('Model_ext', 1))
        year_col = get_column_letter(headers.get('ModelYear', 1))
        year_ext_col = get_column_letter(headers.get('ModelYear_ext', 1))
        
        if include_mask_in_main:
            # Use the match columns directly from the main sheet with dynamic formulas as backup
            format_mappings = [
                ('Make_ext', 'Make Match', make_col, make_ext_col),      # Make_ext column uses Make Match column
                ('Model_ext', 'Model Match', model_col, model_ext_col),    # Model_ext column uses Model Match column  
                ('ModelYear_ext', 'Year Match', year_col, year_ext_col)  # ModelYear_ext column uses Year Match column
            ]
            
            for target_col, match_col, source_col, target_col_letter in format_mappings:
                if target_col in headers:
                    target_letter = get_column_letter(headers[target_col])
                    cell_range = f"{target_letter}2:{target_letter}{max_row}"
                    
                    if match_col in headers:
                        # Use the boolean match columns if they exist
                        match_letter = get_column_letter(headers[match_col])
                        green_rule = FormulaRule(formula=[f"=${match_letter}2=TRUE"], fill=mint_green)
                        pink_rule = FormulaRule(formula=[f"=${match_letter}2=FALSE"], fill=coral_pink)
                    else:
                        # Fallback to dynamic formula comparison if match columns don't exist
                        if target_col == 'ModelYear_ext':
                            # Year comparison as text
                            green_formula = f"=TEXT({source_col}2,\"0\")=TEXT({target_letter}2,\"0\")"
                            pink_formula = f"=TEXT({source_col}2,\"0\")<>TEXT({target_letter}2,\"0\")"
                        else:
                            # Make/Model comparison case-insensitive
                            green_formula = f"=UPPER({source_col}2)=UPPER({target_letter}2)"
                            pink_formula = f"=UPPER({source_col}2)<>UPPER({target_letter}2)"
                        
                        green_rule = FormulaRule(formula=[green_formula], fill=mint_green)
                        pink_rule = FormulaRule(formula=[pink_formula], fill=coral_pink)
                    
                    worksheet.conditional_formatting.add(cell_range, green_rule)
                    worksheet.conditional_formatting.add(cell_range, pink_rule)
        else:
            # Create separate Mask sheet for dynamic formulas (original behavior)
            mask_ws = workbook.create_sheet("Mask")
            mask_ws['A1'] = 'MakeMatch'
            mask_ws['B1'] = 'ModelMatch'  
            mask_ws['C1'] = 'YearMatch'
            
            # Build mask formulas - directly compare the data columns
            for r in range(2, max_row + 1):
                # Make match formula - compare Make vs Make_ext (case-insensitive)
                mask_ws[f"A{r}"] = f"=UPPER(Analysis!{make_col}{r})=UPPER(Analysis!{make_ext_col}{r})"
                # Model match formula - compare Model vs Model_ext (case-insensitive)
                mask_ws[f"B{r}"] = f"=UPPER(Analysis!{model_col}{r})=UPPER(Analysis!{model_ext_col}{r})"
                # Year match formula - compare ModelYear vs ModelYear_ext (as text)
                mask_ws[f"C{r}"] = f"=TEXT(Analysis!{year_col}{r},\"0\")=TEXT(Analysis!{year_ext_col}{r},\"0\")"
            
            # Apply conditional formatting to specific columns using dynamic formulas
            format_mappings = [
                ('Make_ext', make_col, make_ext_col),      # Make_ext column compared to Make column
                ('Model_ext', model_col, model_ext_col),   # Model_ext column compared to Model column
                ('ModelYear_ext', year_col, year_ext_col)  # ModelYear_ext column compared to ModelYear column
            ]
            
            for target_col, source_col, target_col_letter in format_mappings:
                if target_col in headers:
                    target_letter = get_column_letter(headers[target_col])
                    cell_range = f"{target_letter}2:{target_letter}{max_row}"
                    
                    # Green for matches - dynamic formula comparing columns
                    if target_col == 'ModelYear_ext':
                        # Year comparison as text
                        green_formula = f"=TEXT({source_col}2,\"0\")=TEXT({target_letter}2,\"0\")"
                        pink_formula = f"=TEXT({source_col}2,\"0\")<>TEXT({target_letter}2,\"0\")"
                    else:
                        # Make/Model comparison case-insensitive
                        green_formula = f"=UPPER({source_col}2)=UPPER({target_letter}2)"
                        pink_formula = f"=UPPER({source_col}2)<>UPPER({target_letter}2)"
                    
                    green_rule = FormulaRule(formula=[green_formula], fill=mint_green)
                    worksheet.conditional_formatting.add(cell_range, green_rule)
                    
                    pink_rule = FormulaRule(formula=[pink_formula], fill=coral_pink)
                    worksheet.conditional_formatting.add(cell_range, pink_rule)

    def get_verification_summary(self) -> Dict[str, Union[int, float]]:
        """Get summary statistics from verification"""
        if self.merged_data is None:
            return {}

        # Calculate matches dynamically
        make_matches_calc = (
            self.merged_data['Make'].str.lower() == 
            self.merged_data['Make_ext'].str.lower()
        )
        model_matches_calc = (
            self.merged_data['Model'].str.lower() == 
            self.merged_data['Model_ext'].str.lower()
        )
        year_matches_calc = (
            self.merged_data['ModelYear'].astype(str) == 
            self.merged_data['ModelYear_ext'].astype(str)
        )

        total_records = len(self.merged_data)
        make_matches = make_matches_calc.sum()
        model_matches = model_matches_calc.sum()
        year_matches = year_matches_calc.sum()

        return {
            'total_records': total_records,
            'make_matches': make_matches,
            'model_matches': model_matches,
            'year_matches': year_matches,
            'make_match_percentage': make_matches / total_records * 100 if total_records > 0 else 0,
            'model_match_percentage': model_matches / total_records * 100 if total_records > 0 else 0,
            'year_match_percentage': year_matches / total_records * 100 if total_records > 0 else 0
        }

    def get_sample_mismatches(self, n: int = 5) -> pd.DataFrame:
        """Get sample mismatches for display"""
        if self.merged_data is None:
            return pd.DataFrame()

        # Calculate mismatches dynamically
        make_matches = (
            self.merged_data['Make'].str.lower() == 
            self.merged_data['Make_ext'].str.lower()
        )
        model_matches = (
            self.merged_data['Model'].str.lower() == 
            self.merged_data['Model_ext'].str.lower()
        )
        year_matches = (
            self.merged_data['ModelYear'].astype(str) == 
            self.merged_data['ModelYear_ext'].astype(str)
        )

        mismatches = self.merged_data[
            ~make_matches |
            ~model_matches |
            ~year_matches
        ]

        if len(mismatches) == 0:
            return pd.DataFrame()

        sample_cols = ['VIN', 'Make', 'Make_ext', 'Model', 'Model_ext', 'ModelYear', 'ModelYear_ext']
        return mismatches[sample_cols].head(n)
