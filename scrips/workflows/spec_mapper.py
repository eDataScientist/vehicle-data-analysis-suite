import pandas as pd
from thefuzz import fuzz, process
import io
import re
import logging
from tqdm import tqdm
from typing import Dict, List, Tuple, Union, Optional


class ColumnConfig:
    """Configuration class for column mappings."""

    def __init__(self, input_make: str, input_model: str, input_trim: str,
                 ref_make: str, ref_model: str, ref_trim: str):
        # Input file columns
        self.input_make = input_make
        self.input_model = input_model
        self.input_trim = input_trim

        # Reference file columns
        self.ref_make = ref_make
        self.ref_model = ref_model
        self.ref_trim = ref_trim


class SpecMapper:
    """Specification mapper with fuzzy matching and special brand handling"""

    def __init__(self):
        self.input_data = None
        self.reference_data = None
        self.input_sheets = None
        self.reference_sheets = None
        self._mercedes_catalog = None
        self._bmw_catalog = None

    def load_input_data(self, file_data: Union[io.BytesIO, str], file_type: str = 'csv') -> List[str]:
        """Load input data file and return available sheets"""
        if file_type in ['xlsx', 'xls']:
            xls = pd.ExcelFile(file_data)
            self.input_sheets = xls.sheet_names
            return self.input_sheets
        else:
            self.input_data = pd.read_csv(file_data)
            self.input_sheets = ['Data']
            return ['Data']

    def select_input_sheet(self, sheet_name: str, file_data: Union[io.BytesIO, str], file_type: str = 'csv') -> pd.DataFrame:
        """Select and load a specific sheet from input data"""
        if file_type in ['xlsx', 'xls']:
            xls = pd.ExcelFile(file_data)
            self.input_data = xls.parse(sheet_name)
        return self.input_data

    def load_reference_data(self, file_data: Union[io.BytesIO, str], file_type: str = 'csv') -> List[str]:
        """Load reference data file and return available sheets"""
        if file_type in ['xlsx', 'xls']:
            xls = pd.ExcelFile(file_data)
            self.reference_sheets = xls.sheet_names
            return self.reference_sheets
        else:
            self.reference_data = pd.read_csv(file_data)
            self.reference_sheets = ['Data']
            return ['Data']

    def select_reference_sheet(self, sheet_name: str, file_data: Union[io.BytesIO, str], file_type: str = 'csv') -> pd.DataFrame:
        """Select and load a specific sheet from reference data"""
        if file_type in ['xlsx', 'xls']:
            xls = pd.ExcelFile(file_data)
            self.reference_data = xls.parse(sheet_name)
        return self.reference_data

    def get_input_columns(self) -> List[str]:
        """Get list of available columns from input data"""
        if self.input_data is not None:
            return list(self.input_data.columns)
        return []

    def get_reference_columns(self) -> List[str]:
        """Get list of available columns from reference data"""
        if self.reference_data is not None:
            return list(self.reference_data.columns)
        return []

    @staticmethod
    def sanitize_string(s: str) -> str:
        """Sanitize a string by converting to uppercase and normalizing"""
        if not isinstance(s, str):
            return s

        # Convert to uppercase
        s = s.upper()

        # Handle manufacturer abbreviations first
        manufacturer_replacements = {
            'B.M.W': 'BMW',
            'B.M.W.': 'BMW',
            'G.M.C': 'GMC',
            'G.M.C.': 'GMC',
        }

        # Handle specific exact replacements
        s = s.strip()
        if s in ('MERCEDES', 'BENZ'):
            s = 'MERCEDES BENZ'

        for old, new in manufacturer_replacements.items():
            if old in s:
                s = s.replace(old, new)

        # Replace specific patterns
        replacements = {
            '-': ' ', '_': ' ', '/': ' ', '\\': ' ', '(': ' ', ')': ' ',
            '[': ' ', ']': ' ', '{': ' ', '}': ' ', ';': ' ', ':': ' ',
            '.': '', ',': '', '&': 'AND', '+': 'PLUS', '@': 'AT',
            '#': '', '$': '', '*': '', '!': '', '?': '', '=': '',
            '<': '', '>': '', '|': '', '`': '', "'": '', '"': '',
            '~': '', '^': '',
        }

        for old, new in replacements.items():
            s = s.replace(old, new)

        # Normalize whitespace
        s = ' '.join(s.split())
        return s.strip()

    @staticmethod
    def clean_extracted_model_name(model_name: str) -> str:
        """Clean extracted model name by stripping brand names and whitespace."""
        normalized = SpecMapper._normalize_for_special_matching(model_name)
        if not normalized:
            return ""

        # Remove brand prefixes in both spaced and compact forms.
        normalized = re.sub(
            r'^(MERCEDES\s*BENZ|MERCEDESBENZ|MERCEDES|BENZ|BMW)\s*',
            '',
            normalized
        ).strip()
        normalized = re.sub(
            r'\b(MERCEDES\s*BENZ|MERCEDESBENZ|MERCEDES|BENZ|BMW)\b',
            ' ',
            normalized
        )
        normalized = ' '.join(normalized.split()).strip()

        compact = SpecMapper._compact_for_match(normalized)
        for brand in ("MERCEDESBENZ", "MERCEDES", "BENZ", "BMW"):
            if compact.startswith(brand):
                compact = compact[len(brand):]
                break

        if normalized:
            return normalized
        return compact

    @staticmethod
    def _safe_string(value: object) -> str:
        """Convert arbitrary values to clean strings safely."""
        if value is None:
            return ""
        try:
            if pd.isna(value):
                return ""
        except Exception:
            pass
        return str(value).strip()

    @staticmethod
    def _normalize_for_special_matching(value: object) -> str:
        """Normalize strings for special make model/trim matching."""
        text = SpecMapper._safe_string(value)
        if not text:
            return ""
        return SpecMapper.sanitize_string(text)

    @staticmethod
    def _compact_for_match(value: str) -> str:
        """Create a compact alphanumeric representation for dirty/no-space input matching."""
        return re.sub(r'[^A-Z0-9]+', '', value or "")

    def _build_model_aliases(self, model: str, brand_key: str) -> List[str]:
        """Generate aliases for robust model detection."""
        model = self._normalize_for_special_matching(model)
        aliases = {model}

        if model.endswith(" CLASS"):
            base = model[:-len(" CLASS")].strip()
            if base:
                aliases.add(base)
            if brand_key == "MERCEDES" and base == "M":
                aliases.add("ML")

        if model.endswith(" SERIES"):
            base = model[:-len(" SERIES")].strip()
            if base:
                aliases.add(base)

        # AMG GT family is stored under model "AMG" in the reference.
        if brand_key == "MERCEDES" and model == "AMG":
            aliases.update({"AMG GT", "GT"})

        return sorted(
            [a for a in aliases if a],
            key=lambda a: len(self._compact_for_match(a)),
            reverse=True
        )

    def _build_special_brand_catalog(self, processed_reference_df: pd.DataFrame,
                                     make_column: str, model_column: str, trim_column: str,
                                     make_pattern: str, brand_key: str) -> Dict[str, object]:
        """Build model/trim catalogs for special brand extraction from reference data."""
        make_mask = processed_reference_df[make_column].astype(str).str.contains(
            make_pattern, case=False, na=False
        )
        brand_df = processed_reference_df.loc[make_mask, [model_column, trim_column]].copy()

        if brand_df.empty:
            return {"models": [], "trims_by_model": {}, "model_aliases": {}}

        brand_df[model_column] = brand_df[model_column].apply(self._normalize_for_special_matching)
        brand_df[trim_column] = brand_df[trim_column].apply(self._normalize_for_special_matching)
        brand_df = brand_df[brand_df[model_column] != ""]

        models = sorted(
            list(dict.fromkeys(brand_df[model_column].tolist())),
            key=lambda m: len(self._compact_for_match(m)),
            reverse=True
        )

        trims_by_model: Dict[str, List[str]] = {}
        for model, group in brand_df.groupby(model_column):
            trims = [
                t for t in group[trim_column].tolist()
                if t and t not in {"NAN", "NONE"}
            ]
            trims = list(dict.fromkeys(trims))
            trims.sort(key=lambda t: len(self._compact_for_match(t)), reverse=True)
            trims_by_model[model] = trims

        model_aliases = {
            model: self._build_model_aliases(model, brand_key)
            for model in models
        }

        return {
            "models": models,
            "trims_by_model": trims_by_model,
            "model_aliases": model_aliases
        }

    @staticmethod
    def process_df(df: pd.DataFrame, columns: Dict[str, str]) -> pd.DataFrame:
        """Process dataframe by cleaning and standardizing the data"""
        df = df.copy()

        for std_col, actual_col in columns.items():
            if actual_col in df.columns:
                df[actual_col] = df[actual_col].astype(str)
                df[actual_col] = df[actual_col].apply(
                    SpecMapper.sanitize_string)
                if std_col == 'trim':
                    df[actual_col] = df[actual_col].replace(
                        ['NAN', 'NONE', 'nan', 'STANDARD'], 'STANDARD')

        return df

    @staticmethod
    def get_scorer(method: str = "default", simple: bool = False):
        """Get the primary scorer function based on method and simple parameters"""
        method_mapping = {
            "ratio": fuzz.ratio,
            "partial_ratio": fuzz.partial_ratio,
            "token_sort_ratio": fuzz.token_sort_ratio,
            "token_set_ratio": fuzz.token_set_ratio,
            "partial_token_sort_ratio": fuzz.partial_token_sort_ratio,
            "partial_token_set_ratio": fuzz.partial_token_set_ratio
        }

        if method == "default":
            return fuzz.ratio if simple else fuzz.token_set_ratio
        elif method in method_mapping:
            return method_mapping[method]
        else:
            raise ValueError(f"Unknown fuzzy matching method: {method}")

    @staticmethod
    def find_match_fuzzy(input_value: str, reference_list: List[str],
                         threshold: int = 80, simple: bool = False, method: str = "default") -> Tuple[str, Optional[str], int]:
        """Find the best fuzzy match for an input value"""
        if input_value in reference_list:
            return (input_value, input_value, 100)

        # Define available fuzzy matching methods
        method_mapping = {
            "ratio": fuzz.ratio,
            "partial_ratio": fuzz.partial_ratio,
            "token_sort_ratio": fuzz.token_sort_ratio,
            "token_set_ratio": fuzz.token_set_ratio,
            "partial_token_sort_ratio": fuzz.partial_token_sort_ratio,
            "partial_token_set_ratio": fuzz.partial_token_set_ratio
        }

        if method == "default":
            # Use original logic
            if simple:
                best_match = process.extractOne(
                    input_value, reference_list, scorer=fuzz.ratio)
                if best_match and best_match[1] >= threshold:
                    return (input_value, best_match[0], best_match[1])
            else:
                best_match = process.extractOne(
                    input_value, reference_list, scorer=fuzz.token_set_ratio)
                if best_match and best_match[1] >= threshold:
                    return (input_value, best_match[0], best_match[1])

                best_match = process.extractOne(
                    input_value, reference_list, scorer=fuzz.partial_token_set_ratio)
                if best_match and best_match[1] >= threshold:
                    return (input_value, best_match[0], best_match[1])
        else:
            # Use specified method
            if method in method_mapping:
                scorer = method_mapping[method]
                best_match = process.extractOne(
                    input_value, reference_list, scorer=scorer)
                if best_match and best_match[1] >= threshold:
                    return (input_value, best_match[0], best_match[1])
            else:
                raise ValueError(f"Unknown fuzzy matching method: {method}")

        return (input_value, None, -1)

    def _score_special_model(self, input_normalized: str, input_compact: str,
                             model: str, aliases: List[str], brand_key: str) -> int:
        """Score a model candidate against cleaned input."""
        score = -1

        for alias in aliases:
            alias_compact = self._compact_for_match(alias)
            if not alias_compact:
                continue

            if input_compact == alias_compact or input_normalized == alias:
                score = max(score, 1000 + len(alias_compact))

            if input_compact.startswith(alias_compact):
                score = max(score, 950 + len(alias_compact))

            if len(alias_compact) >= 3 and alias_compact in input_compact:
                score = max(score, 850 + len(alias_compact))

            if len(alias_compact) >= 2 and re.search(rf'\b{re.escape(alias)}\b', input_normalized):
                score = max(score, 900 + len(alias_compact))

        # BMW numeric series handling for inputs like 320I, M340I, M3.
        if brand_key == "BMW" and model.endswith(" SERIES"):
            series_digit = model.split()[0]
            if re.search(rf'(^|[^A-Z0-9]){series_digit}SERIES', input_compact):
                score = max(score, 920)
            if re.search(rf'(^|[^A-Z0-9])M?{series_digit}\d{{2,3}}', input_compact):
                score = max(score, 840)
            if re.search(rf'(^|[^A-Z0-9])M{series_digit}(?!\d)', input_compact):
                score = max(score, 835)

        # Mercedes class handling for compact forms like C200 / GLC300 / ML350.
        if brand_key == "MERCEDES" and model.endswith(" CLASS"):
            base = model[:-len(" CLASS")].strip()
            base_compact = self._compact_for_match(base)

            if base == "M" and re.search(r'(^|[^A-Z0-9])ML\d{2,3}', input_compact):
                score = max(score, 910)

            if len(base_compact) >= 2:
                if f"{base_compact}CLASS" in input_compact:
                    score = max(score, 915 + len(base_compact))
                if re.search(rf'{re.escape(base_compact)}\d{{2,3}}', input_compact):
                    score = max(score, 845 + len(base_compact))
            elif len(base_compact) == 1:
                if re.search(rf'(^|[^A-Z0-9]){re.escape(base_compact)}CLASS', input_compact):
                    score = max(score, 910)
                if re.search(rf'(^|[^A-Z0-9]){re.escape(base_compact)}\d{{2,3}}', input_compact):
                    score = max(score, 830)

        return score

    def _score_special_trim(self, input_normalized: str, input_compact: str, trim: str) -> int:
        """Score a trim candidate against cleaned input."""
        trim_compact = self._compact_for_match(trim)
        if not trim_compact:
            return -1

        score = -1

        if input_compact == trim_compact or input_normalized == trim:
            score = max(score, 1000 + len(trim_compact))

        if input_compact.endswith(trim_compact):
            score = max(score, 960 + len(trim_compact))

        if input_compact.startswith(trim_compact):
            score = max(score, 930 + len(trim_compact))

        if " " in input_normalized and input_normalized.endswith(trim):
            score = max(score, 970 + len(trim_compact))

        if re.search(rf'\b{re.escape(trim)}\b', input_normalized):
            score = max(score, 940 + len(trim_compact))

        if len(trim_compact) >= 3 and trim_compact in input_compact:
            score = max(score, 860 + len(trim_compact))

        return score

    def _extract_special_model_and_trim(self, model_designation: object, input_trim: object,
                                        catalog: Dict[str, object], brand_key: str,
                                        include_trim: bool = True) -> Optional[str]:
        """Extract model (and optionally trim) for BMW/Mercedes using reference-driven catalogs."""
        if not catalog or not catalog.get("models"):
            return None

        cleaned_model = self.clean_extracted_model_name(model_designation)
        normalized_trim = self._normalize_for_special_matching(input_trim)

        pieces = [cleaned_model]
        if normalized_trim and normalized_trim not in {"NAN", "NONE", "STANDARD"}:
            pieces.append(normalized_trim)

        combined = " ".join([p for p in pieces if p]).strip()
        input_normalized = self._normalize_for_special_matching(combined)
        input_compact = self._compact_for_match(input_normalized)

        if not input_compact:
            return None

        best_model = None
        best_model_score = -1

        for model in catalog["models"]:
            aliases = catalog["model_aliases"].get(model, [model])
            score = self._score_special_model(
                input_normalized, input_compact, model, aliases, brand_key
            )
            if score > best_model_score:
                best_model_score = score
                best_model = model

        if best_model is None or best_model_score < 0:
            return None

        if not include_trim:
            return best_model

        best_trim = None
        best_trim_score = -1
        for trim in catalog["trims_by_model"].get(best_model, []):
            score = self._score_special_trim(input_normalized, input_compact, trim)
            if score > best_trim_score:
                best_trim_score = score
                best_trim = trim

        # Fuzzy fallback on trim value only when direct extraction fails.
        if best_trim is None and normalized_trim and normalized_trim not in {"NAN", "NONE"}:
            trim_candidates = catalog["trims_by_model"].get(best_model, [])
            _, fuzzy_trim, _ = self.find_match_fuzzy(
                normalized_trim, trim_candidates, threshold=70, simple=False, method="default"
            )
            best_trim = fuzzy_trim

        if best_trim:
            return f"{best_model} | {best_trim}"

        return best_model

    def extract_mercedes_class_and_trim(self, model_designation, include_trim=True,
                                        input_trim=None, catalog=None):
        """Extract Mercedes model/trim from free-form input."""
        return self._extract_special_model_and_trim(
            model_designation=model_designation,
            input_trim=input_trim,
            catalog=catalog or self._mercedes_catalog or {},
            brand_key="MERCEDES",
            include_trim=include_trim
        )

    def extract_bmw_series_and_trim(self, model_designation, include_trim=True,
                                    input_trim=None, catalog=None):
        """Extract BMW model/trim from free-form input."""
        return self._extract_special_model_and_trim(
            model_designation=model_designation,
            input_trim=input_trim,
            catalog=catalog or self._bmw_catalog or {},
            brand_key="BMW",
            include_trim=include_trim
        )

    def map_specifications(self, column_config: ColumnConfig, skip_trim: bool = False,
                           make_threshold: int = 80, model_threshold: int = 80,
                           trim_threshold: int = 80, use_original_on_no_match: bool = False,
                           method: str = "default", method_make: str = None, method_model: str = None,
                           method_trim: str = None, skip_special_brands: bool = False,
                           use_gemini_verification: bool = False) -> Dict[str, pd.DataFrame]:
        """Map specifications from input dataframe to reference dataframe format

        Args:
            skip_special_brands: If True, process Mercedes/BMW as standard vehicles (no special extraction)
        """

        # Use dimension-specific methods if provided, otherwise use the single method parameter for backwards compatibility
        if method_make is None:
            method_make = method
        if method_model is None:
            method_model = method
        if method_trim is None:
            method_trim = method

        print("\n=== DEBUG: Starting map_specifications ===")
        print(f"Skip trim: {skip_trim}")
        print(f"Skip special brands (Mercedes/BMW): {skip_special_brands}")
        print(f"Thresholds - Make: {make_threshold}, Model: {model_threshold}, Trim: {trim_threshold}")
        print(f"Use original on no match: {use_original_on_no_match}")
        print(f"Fuzzy matching methods - Make: {method_make}, Model: {method_model}, Trim: {method_trim}")

        if self.input_data is None or self.reference_data is None:
            raise ValueError("Both input and reference data must be loaded")

        print(f"Input data shape: {self.input_data.shape}")
        print(f"Reference data shape: {self.reference_data.shape}")

        # Process input and reference DataFrames
        input_columns = {
            'make': column_config.input_make,
            'model': column_config.input_model,
            'trim': column_config.input_trim
        }
        ref_columns = {
            'make': column_config.ref_make,
            'model': column_config.ref_model,
            'trim': column_config.ref_trim
        }

        processed_input_df = self.process_df(self.input_data, input_columns)
        processed_reference_df = self.process_df(
            self.reference_data, ref_columns)

        # Build special brand catalogs from the actual reference specification.
        self._mercedes_catalog = self._build_special_brand_catalog(
            processed_reference_df=processed_reference_df,
            make_column=column_config.ref_make,
            model_column=column_config.ref_model,
            trim_column=column_config.ref_trim,
            make_pattern='MERCEDES',
            brand_key='MERCEDES'
        )
        self._bmw_catalog = self._build_special_brand_catalog(
            processed_reference_df=processed_reference_df,
            make_column=column_config.ref_make,
            model_column=column_config.ref_model,
            trim_column=column_config.ref_trim,
            make_pattern='BMW',
            brand_key='BMW'
        )

        print(
            f"Special catalogs - Mercedes models: {len(self._mercedes_catalog.get('models', []))}, "
            f"BMW models: {len(self._bmw_catalog.get('models', []))}"
        )

        # Get unique makes
        reference_makes = processed_reference_df[column_config.ref_make].unique(
        ).tolist()
        reference_makes = [
            make for make in reference_makes if isinstance(make, str)]

        input_makes = processed_input_df[column_config.input_make].unique(
        ).tolist()
        input_makes = [make for make in input_makes if isinstance(make, str)]

        # Map makes
        mapped_makes, unmatched_makes = self._map_makes(
            input_makes, reference_makes, make_threshold, method_make, use_gemini_verification)

        # Join with input data
        joined_input = processed_input_df.merge(
            mapped_makes, left_on=column_config.input_make, right_on='Input Make', how='left')
        joined_input = joined_input.drop(columns=['Input Make', 'Score'])

        # Handle Mercedes/BMW vehicles with special extraction (unless disabled)
        mercedes_models_df = pd.DataFrame()
        mercedes_trims_df = pd.DataFrame()
        unmapped_mercedes_df = pd.DataFrame()
        bmw_models_df = pd.DataFrame()
        bmw_trims_df = pd.DataFrame()
        unmapped_bmw_df = pd.DataFrame()

        # Initialize these to avoid reference errors when skip_special_brands=True
        mercedes_vehicles = pd.DataFrame()
        bmw_vehicles = pd.DataFrame()

        # Track failed extractions for fallback to standard mapping
        failed_mercedes_models = set()
        failed_bmw_models = set()

        if not skip_special_brands:
            # Handle Mercedes vehicles
            mercedes_mask = joined_input['Mapped Make'].str.contains(
                'MERCEDES', case=False, na=False)
            mercedes_vehicles = joined_input[mercedes_mask].copy()

            if not mercedes_vehicles.empty:
                mercedes_models_df, mercedes_trims_df, unmapped_mercedes_df = self._process_mercedes_vehicles(
                    mercedes_vehicles, column_config, skip_trim, self._mercedes_catalog)
                joined_input.loc[mercedes_mask] = mercedes_vehicles.values

                # Track failed extractions for fallback
                if not unmapped_mercedes_df.empty and 'Input Model' in unmapped_mercedes_df.columns:
                    failed_mercedes_models = set(unmapped_mercedes_df['Input Model'].tolist())
                    print(f"Mercedes: {len(failed_mercedes_models)} models failed extraction, will try standard mapping")

            # Handle BMW vehicles
            non_mercedes_vehicles = joined_input[~mercedes_mask].copy()
            bmw_mask = non_mercedes_vehicles['Mapped Make'].str.contains(
                'BMW', case=False, na=False)
            bmw_vehicles = non_mercedes_vehicles[bmw_mask].copy()

            if not bmw_vehicles.empty:
                bmw_models_df, bmw_trims_df, unmapped_bmw_df = self._process_bmw_vehicles(
                    bmw_vehicles, column_config, skip_trim, self._bmw_catalog)
                joined_input.loc[non_mercedes_vehicles.index[bmw_mask]
                                 ] = bmw_vehicles.values

                # Track failed extractions for fallback
                if not unmapped_bmw_df.empty and 'Input Model' in unmapped_bmw_df.columns:
                    failed_bmw_models = set(unmapped_bmw_df['Input Model'].tolist())
                    print(f"BMW: {len(failed_bmw_models)} models failed extraction, will try standard mapping")

            # Get non-special vehicles PLUS failed special brand extractions for fallback
            non_special_vehicles = non_mercedes_vehicles[~bmw_mask].copy()

            # Add failed Mercedes/BMW extractions to standard processing
            failed_special_mask = (
                (joined_input['Mapped Make'].str.contains('MERCEDES', case=False, na=False) &
                 joined_input[column_config.input_model].isin(failed_mercedes_models)) |
                (joined_input['Mapped Make'].str.contains('BMW', case=False, na=False) &
                 joined_input[column_config.input_model].isin(failed_bmw_models))
            )
            failed_special_vehicles = joined_input[failed_special_mask].copy()

            if not failed_special_vehicles.empty:
                print(f"Adding {len(failed_special_vehicles)} failed special brand vehicles to standard mapping")
                non_special_vehicles = pd.concat([non_special_vehicles, failed_special_vehicles], ignore_index=False)
        else:
            print("\n=== Special brand processing DISABLED - treating Mercedes/BMW as standard vehicles ===")
            # All vehicles are standard when special processing is disabled
            non_special_vehicles = joined_input.copy()

        # Handle standard models
        make_models = processed_reference_df.groupby(
            column_config.ref_make)[column_config.ref_model].unique().apply(list).to_dict()

        make_models_worded = {}
        # This is a reverse mapping from worded model names to original model names
        # Keyed by (worded_model, make) to avoid cross-make collisions
        reverse_mapping = {}

        for make, model_set in make_models.items():
            worded_models = self._wordify_models(model_set, make=make)
            for orig, worded in worded_models.items():
                reverse_mapping[(worded, make)] = orig
            worded_models_list = list(worded_models.values())
            make_models_worded[make] = worded_models_list

        print("DEBUG: Prepared make_models_worded for standard models mapping")

        if not non_special_vehicles.empty:
            # Collect unique (model, make) pairs instead of a dictionary to avoid
            # cross-make overwrites (e.g. S5 exists for AUDI, JAC, LUXGEN)
            input_model_make_pairs = set()
            for _, row in non_special_vehicles.iterrows():
                model = row[column_config.input_model]
                make = row['Mapped Make']
                if isinstance(model, str) and isinstance(make, str):
                    input_model_make_pairs.add((model, make))

            # Group input models by make for proper year pattern handling
            input_models_by_make = {}
            for model, make in input_model_make_pairs:
                if make not in input_models_by_make:
                    input_models_by_make[make] = []
                input_models_by_make[make].append(model)

            # Process each make's models separately with year pattern awareness
            # worded_input_models_set: set of (worded_model, make)
            worded_input_models_set = set()
            # reverse_input_mappings: (worded_model, make) -> original_model
            reverse_input_mappings = {}
            for make, models in input_models_by_make.items():
                worded = self._wordify_models(models, make=make)
                for orig_model, worded_model in worded.items():
                    reverse_input_mappings[(worded_model, make)] = orig_model
                    worded_input_models_set.add((worded_model, make))

            print(f"DEBUG: Prepared worded_input_models_set for standard models mapping - {len(worded_input_models_set)} entries")
            
        else:
            input_model_make_pairs = set()
            worded_input_models_set = set()
            reverse_input_mappings = {}

        mapped_models_standard, unmatched_models_standard = self._map_models(
            worded_input_models_set, make_models_worded, model_threshold, method_model)

        # CRITICAL: Merge standard mapped models back into joined_input for trim mapping
        # Mercedes/BMW models were already updated in-place, but standard models need to be merged
        if not mapped_models_standard.empty:
            print(f"\n=== DEBUG: Merging standard models back into joined_input ===")
            print(f"Standard models to merge: {len(mapped_models_standard)}")

            # Ensure Mapped Model column exists
            if 'Mapped Model' not in joined_input.columns:
                 joined_input['Mapped Model'] = None
                 print("Created 'Mapped Model' column in joined_input")

            # Create a lookup dict for faster merging
            # Keys are (original_input_model, make) tuples, values are original reference model names
            standard_model_lookup = {}
            for _, mrow in mapped_models_standard.iterrows():
                worded_input = mrow['Input Model']
                worded_ref = mrow['Mapped Model']
                make = mrow['Master Make']
                orig_input = reverse_input_mappings.get((worded_input, make), worded_input)
                orig_ref = reverse_mapping.get((worded_ref, make), worded_ref)
                standard_model_lookup[(orig_input, make)] = orig_ref

            # Update joined_input with mapped models for non-Mercedes/BMW vehicles
            for idx, row in joined_input.iterrows():
                if pd.isna(row.get('Mapped Model', None)):  # Only update if not already set by Mercedes/BMW
                    input_model = row[column_config.input_model]
                    mapped_make = row.get('Mapped Make', None)
                    if (input_model, mapped_make) in standard_model_lookup:
                        joined_input.at[idx, 'Mapped Model'] = standard_model_lookup[(input_model, mapped_make)]

            print(f"joined_input rows with Mapped Model after merge: {joined_input['Mapped Model'].notna().sum()}")

        # Combine all model mappings - convert worded names back to originals using tuple-keyed dicts
        if not mapped_models_standard.empty:
            mapped_models_standard['Input Model'] = mapped_models_standard.apply(
                lambda r: reverse_input_mappings.get((r['Input Model'], r['Master Make']), r['Input Model']), axis=1)
            mapped_models_standard['Mapped Model'] = mapped_models_standard.apply(
                lambda r: reverse_mapping.get((r['Mapped Model'], r['Master Make']), r['Mapped Model']), axis=1)

        # Identify which failed special brand models were successfully mapped via fallback
        fallback_success_models = set()
        if not mapped_models_standard.empty:
            fallback_success_models = set(mapped_models_standard['Input Model']) & (failed_mercedes_models | failed_bmw_models)
            if fallback_success_models:
                print(f"Fallback success: {len(fallback_success_models)} failed special brand models mapped via standard fuzzy matching")

        mapped_models = pd.concat([mapped_models_standard, mercedes_models_df, bmw_models_df],
                                  ignore_index=True)

        print('DEBUG UnMapped Models Standard Columns:', unmatched_models_standard.columns)
        if not unmatched_models_standard.empty:
            unmatched_models_standard['Input Model'] = unmatched_models_standard.apply(
                lambda r: reverse_input_mappings.get((r['Input Model'], r['Master Make']), r['Input Model']), axis=1)
            unmatched_models_standard['Best Match'] = unmatched_models_standard.apply(
                lambda r: reverse_mapping.get((r['Best Match'], r['Master Make']), r['Best Match']) if r['Best Match'] is not None else None, axis=1)

        # Remove fallback successes from unmapped special brand lists
        if fallback_success_models:
            if not unmapped_mercedes_df.empty and 'Input Model' in unmapped_mercedes_df.columns:
                unmapped_mercedes_df = unmapped_mercedes_df[~unmapped_mercedes_df['Input Model'].isin(fallback_success_models)]
                print(f"Removed {len(fallback_success_models & failed_mercedes_models)} Mercedes models from unmapped (successfully mapped via fallback)")
            if not unmapped_bmw_df.empty and 'Input Model' in unmapped_bmw_df.columns:
                unmapped_bmw_df = unmapped_bmw_df[~unmapped_bmw_df['Input Model'].isin(fallback_success_models)]
                print(f"Removed {len(fallback_success_models & failed_bmw_models)} BMW models from unmapped (successfully mapped via fallback)")

        unmatched_models = pd.concat([unmatched_models_standard, unmapped_mercedes_df, unmapped_bmw_df],
                                     ignore_index=True)

        if not skip_special_brands:
            print(f"\n=== DEBUG: Mercedes/BMW Processing Summary ===")
            print(f"Mercedes - Total identified: {len(mercedes_vehicles)}")
            print(f"Mercedes - Successfully mapped: {len(mercedes_models_df)}")
            print(f"Mercedes - Failed extraction: {len(unmapped_mercedes_df)}")
            if not unmapped_mercedes_df.empty:
                print(f"Mercedes - Sample failed models: {unmapped_mercedes_df['Input Model'].head(5).tolist()}")

            print(f"\nBMW - Total identified: {len(bmw_vehicles)}")
            print(f"BMW - Successfully mapped: {len(bmw_models_df)}")
            print(f"BMW - Failed extraction: {len(unmapped_bmw_df)}")
            if not unmapped_bmw_df.empty:
                print(f"BMW - Sample failed models: {unmapped_bmw_df['Input Model'].head(5).tolist()}")

        results = {
            'mapped_makes': mapped_makes,
            'unmatched_makes': unmatched_makes,
            'mapped_models': mapped_models,
            'unmatched_models': unmatched_models
        }

        # Include Gemini verification log if available
        if hasattr(self, 'gemini_verification_log') and self.gemini_verification_log:
            results['gemini_verification_log'] = self.gemini_verification_log

        print(f"\n=== DEBUG: Base results created ===")
        print(f"Mapped makes: {len(mapped_makes)}")
        print(f"Unmatched makes: {len(unmatched_makes)}")
        print(f"Mapped models: {len(mapped_models)}")
        if not skip_special_brands:
            print(f"  - Standard: {len(mapped_models_standard)}")
            print(f"  - Mercedes: {len(mercedes_models_df)}")
            print(f"  - BMW: {len(bmw_models_df)}")
        print(f"Unmatched models: {len(unmatched_models)}")
        if not skip_special_brands:
            print(f"  - Standard: {len(unmatched_models_standard)}")
            print(f"  - Mercedes failed: {len(unmapped_mercedes_df)}")
            print(f"  - BMW failed: {len(unmapped_bmw_df)}")

        if not skip_trim and column_config.input_trim and column_config.ref_trim:
            print(f"\n=== DEBUG: Starting trim mapping ===")
            print(f"Joined input shape: {joined_input.shape}")
            print(f"Trim column: {column_config.input_trim}")

            # Check unique trims in joined_input BEFORE mapping
            if column_config.input_trim in joined_input.columns:
                unique_trims_before = joined_input[column_config.input_trim].nunique()
                total_non_null_trims = joined_input[column_config.input_trim].notna().sum()
                print(f"Unique trims in joined_input BEFORE mapping: {unique_trims_before}")
                print(f"Total non-null trim values: {total_non_null_trims}")
                print(f"Sample trims (first 10): {joined_input[column_config.input_trim].dropna().unique()[:10].tolist()}")

            # Handle trim mapping
            mapped_trims, unmatched_trims = self._map_trims(
                joined_input, processed_reference_df, column_config,
                reference_makes, mercedes_trims_df, bmw_trims_df, trim_threshold, method_trim, skip_special_brands)

            print(f"\n=== DEBUG: Trim mapping results ===")
            print(f"Mapped trims DataFrame: {len(mapped_trims)} rows")
            print(f"Unique mapped trims: {mapped_trims['Input Trim'].nunique() if not mapped_trims.empty else 0}")
            print(f"Sample mapped trims (first 10):")
            if not mapped_trims.empty:
                print(mapped_trims[['Input Trim', 'Mapped Trim', 'Master Make', 'Master Model']].head(10))

            print(f"\nUnmatched trims DataFrame: {len(unmatched_trims)} rows")
            print(f"Unique unmatched trims: {unmatched_trims['Input Trim'].nunique() if not unmatched_trims.empty else 0}")
            print(f"Sample unmatched trims (first 10):")
            if not unmatched_trims.empty:
                print(unmatched_trims[['Input Trim', 'Master Make', 'Master Model']].head(10))

            results['mapped_trims'] = mapped_trims
            results['unmatched_trims'] = unmatched_trims
        else:
            print(f"\n=== DEBUG: Skipping trim mapping (skip_trim={skip_trim}) ===")

        print(f"\n=== DEBUG: map_specifications completed ===")
        print(f"Result keys: {results.keys()}")
        return results
    
    def _wordify_models(self, models: List[str], make: str = None) -> Dict[str, str]:
        """Convert model names with meaningful number-to-word conversion.

        Removes year patterns (19xx, 20xx) unless the model is in the exclusion list.

        Examples:
            'CAMRY 2020' -> 'CAMRY' (year removed)
            'X5 2021' -> 'X FIVE' (year removed, then number converted)
            'BMW 2002' -> 'BMW TWO THOUSAND TWO' (year preserved, it's the actual model name)
            'PEUGEOT 2008' -> 'PEUGEOT TWO THOUSAND EIGHT' (year preserved, it's the actual model name)

        Args:
            models: List of model names to process
            make: Optional make name to check against exclusion list
        """

        # Exhaustive list of cars with year-based model names (19xx or 20xx patterns)
        # These should NOT have their "year" numbers removed as they are the actual model name
        YEAR_PATTERN_MODEL_EXCLUSIONS = {
            'ALFA ROMEO': ['2000', '2600'],
            'BMW': ['2002', '2000'],
            'DATSUN': ['2000'],
            'FIAT': ['1900'],
            'MAZDA': ['2000'],
            'MERCEDES': ['1900'],
            'MERCEDES-BENZ': ['1900'],
            'MERCEDES BENZ': ['1900'],
            'PEUGEOT': ['2008'],
            'ROVER': ['2000'],
            'TRIUMPH': ['2000', '2500'],
            'NISSAN': ['2000'],  # Datsun became Nissan
        }

        def number_to_words(num_str: str) -> str:
            """Convert a numeric string to words meaningfully."""
            num = int(num_str)

            # Handle special cases
            if num == 0:
                return "ZERO"

            # Define word mappings
            ones = ["", "ONE", "TWO", "THREE", "FOUR", "FIVE", "SIX", "SEVEN", "EIGHT", "NINE"]
            tens = ["", "", "TWENTY", "THIRTY", "FORTY", "FIFTY", "SIXTY", "SEVENTY", "EIGHTY", "NINETY"]
            teens = ["TEN", "ELEVEN", "TWELVE", "THIRTEEN", "FOURTEEN", "FIFTEEN",
                     "SIXTEEN", "SEVENTEEN", "EIGHTEEN", "NINETEEN"]

            # Handle 1-9
            if num < 10:
                return ones[num]

            # Handle 10-19
            elif num < 20:
                return teens[num - 10]

            # Handle 20-99
            elif num < 100:
                return (tens[num // 10] + (" " + ones[num % 10] if num % 10 != 0 else "")).strip()

            # Handle 100-999
            elif num < 1000:
                hundreds_part = ones[num // 100] + " HUNDRED"
                remainder = num % 100
                if remainder == 0:
                    return hundreds_part
                elif remainder < 20:
                    return hundreds_part + " " + (teens[remainder - 10] if remainder >= 10 else ones[remainder])
                else:
                    return hundreds_part + " " + (tens[remainder // 10] + (" " + ones[remainder % 10] if remainder % 10 != 0 else "")).strip()

            # Handle 1000-9999
            elif num < 10000:
                thousands_part = ones[num // 1000] + " THOUSAND"
                remainder = num % 1000
                if remainder == 0:
                    return thousands_part
                elif remainder < 100:
                    if remainder < 20 and remainder >= 10:
                        return thousands_part + " " + teens[remainder - 10]
                    elif remainder < 10:
                        return thousands_part + " " + ones[remainder]
                    else:
                        return thousands_part + " " + (tens[remainder // 10] + (" " + ones[remainder % 10] if remainder % 10 != 0 else "")).strip()
                else:
                    return thousands_part + " " + number_to_words(str(remainder))

            # For larger numbers, just return the number as-is
            else:
                return num_str

        def should_preserve_year_pattern(model: str, make: str) -> bool:
            """Check if a model's year pattern should be preserved."""
            # Type checking - handle NaN, None, and non-string types
            if not make or not isinstance(make, str):
                return False
            if not model or not isinstance(model, str):
                return False

            make_upper = make.upper()
            if make_upper not in YEAR_PATTERN_MODEL_EXCLUSIONS:
                return False

            # Extract just the numeric part for comparison
            year_matches = re.findall(r'\b(19\d{2}|20\d{2})\b', model.upper())
            if not year_matches:
                return False

            # Check if any year pattern matches the exclusion list
            exclusion_list = YEAR_PATTERN_MODEL_EXCLUSIONS[make_upper]
            for year in year_matches:
                if year in exclusion_list:
                    return True

            return False

        corrected_models = {}
        for model in models:
            og_model = model

            # Handle NaN, None, and non-string types
            if not isinstance(model, str):
                # Convert to string or use empty string for invalid types
                model = str(model) if model is not None else ""

            # Skip empty models after conversion
            if not model or model.upper() in ['NAN', 'NONE', '']:
                corrected_models[og_model] = ""
                continue

            processed_model = model.upper()

            # Remove year patterns (19xx, 20xx) unless this is a year-based model name
            if not should_preserve_year_pattern(processed_model, make):
                # Remove standalone year patterns (with word boundaries to avoid partial matches)
                processed_model = re.sub(r'\b(19\d{2}|20\d{2})\b', '', processed_model)
                # Clean up extra spaces
                processed_model = ' '.join(processed_model.split())

            # Find all sequences of digits (e.g., "320", "2000", not individual digits)
            number_sequences = re.findall(r'\d+', processed_model)

            # Replace each number sequence with its word equivalent
            worded_model = processed_model
            for num_seq in number_sequences:
                word_form = number_to_words(num_seq)
                worded_model = worded_model.replace(num_seq, f" {word_form} ", 1)

            # Clean up extra spaces and convert to uppercase
            worded_model = ' '.join(worded_model.split()).upper()
            corrected_models[og_model] = worded_model

        return corrected_models

    def _map_makes(self, input_makes: List[str], reference_makes: List[str],
                   threshold: int = 80, method: str = "default",
                   use_gemini_verification: bool = False) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Map makes using fuzzy matching with optional Gemini AI verification"""
        results = []
        unmatched = []
        scorer = self.get_scorer(method=method, simple=True)

        # Phase 1: Fuzzy matching
        for make in tqdm(input_makes, desc="Mapping Makes"):
            input_value, matched_value, score = self.find_match_fuzzy(
                make, reference_makes, threshold, simple=True, method=method)

            if matched_value is not None:
                results.append((input_value, matched_value, score))
            else:
                results.append((input_value, None, -1))
                # Use the same scorer as the matching method
                best_match = process.extractOne(
                    make, reference_makes, scorer=scorer)
                unmatched.append(
                    (make, best_match[0] if best_match else None,
                     best_match[1] if best_match else -1))

        # Phase 2: Gemini AI verification (optional)
        if use_gemini_verification:
            try:
                from ..services.gemini_verification_service import GeminiVerificationService

                # Filter only matched makes (those with non-None mapped value)
                matched_results = [(inp, mapped, score) for inp, mapped, score in results if mapped is not None]

                if matched_results:
                    print(f"\nVerifying {len(matched_results)} matched makes with Gemini AI...")

                    # Initialize service
                    verifier = GeminiVerificationService()

                    # Run async verification
                    import asyncio
                    import nest_asyncio
                    nest_asyncio.apply()

                    verified_results = asyncio.run(
                        verifier.verify_make_mappings_batch(matched_results)
                    )

                    # Separate verified and failed
                    verified_makes = []
                    failed_verification = []
                    verification_log = []

                    for input_make, mapped_make, score, is_verified, reason in verified_results:
                        verification_log.append({
                            'input_make': input_make,
                            'mapped_make': mapped_make,
                            'score': score,
                            'verified': is_verified,
                            'reason': reason
                        })
                        if is_verified:
                            verified_makes.append((input_make, mapped_make, score))
                            print(f"  ✓ {input_make} -> {mapped_make} | {reason}")
                        else:
                            # Failed verification -> move to unmatched
                            print(f"  ✗ {input_make} -> {mapped_make} | {reason}")
                            best_match = process.extractOne(input_make, reference_makes, scorer=scorer)
                            failed_verification.append(
                                (input_make, best_match[0] if best_match else None,
                                 best_match[1] if best_match else -1)
                            )

                    # Store verification log on instance for output
                    self.gemini_verification_log = verification_log

                    print(f"\nGemini verification: {len(verified_makes)} verified, {len(failed_verification)} failed")

                    # Update results - replace matched results with verified ones
                    # Keep unmatched results (those with None mapped value) as-is
                    unmatched_input = [inp for inp, mapped, _ in results if mapped is None]
                    results = verified_makes + [(inp, None, -1) for inp in unmatched_input]
                    unmatched.extend(failed_verification)

            except ImportError:
                print("Gemini verification service not available, skipping...")
            except Exception as e:
                print(f"Error during Gemini verification: {str(e)}, continuing with fuzzy results...")

        mapped_makes = pd.DataFrame(
            results, columns=['Input Make', 'Mapped Make', 'Score'])
        unmatched_makes = pd.DataFrame(
            unmatched, columns=['Input Make', 'Best Match', 'Score'])

        return mapped_makes[mapped_makes['Mapped Make'].notnull()], unmatched_makes

    def _map_models(self, input_models_set: set, make_models: Dict[str, List[str]],
                    threshold: int = 80, method: str = "default") -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Map models using fuzzy matching"""
        results = []
        unmatched = []
        scorer = self.get_scorer(method=method, simple=False)

        for model, master_make in tqdm(input_models_set, desc="Mapping Models"):
            if master_make not in make_models:
                unmatched.append((model, None, -1, master_make))
                continue

            # Default to original model, remove make name if found
            updated_model = model
            if master_make.upper() in model.upper():
                updated_model = model.replace(master_make.upper(), "").strip()

            reference_models = make_models[master_make]
            input_value, matched_value, score = self.find_match_fuzzy(
                updated_model, reference_models, threshold, simple=False, method=method)

            if matched_value is not None:
                results.append(
                    (model, matched_value, score, master_make))
            else:
                results.append((model, None, -1, master_make))
                # Use the same scorer as the matching method
                best_match = process.extractOne(
                    model, reference_models, scorer=scorer)
                unmatched.append(
                    (model, best_match[0] if best_match else None,
                     best_match[1] if best_match else -1, master_make))

        mapped_models = pd.DataFrame(
            results, columns=['Input Model', 'Mapped Model', 'Score', 'Master Make'])
        unmatched_models = pd.DataFrame(
            unmatched, columns=['Input Model', 'Best Match', 'Score', 'Master Make'])

        return mapped_models[mapped_models['Mapped Model'].notnull()], unmatched_models

    def _map_trims(self, joined_input: pd.DataFrame, processed_reference_df: pd.DataFrame,
                   column_config: ColumnConfig, reference_makes: List[str],
                   mercedes_trims_df: pd.DataFrame, bmw_trims_df: pd.DataFrame,
                   threshold: int = 80, method: str = "default", skip_special_brands: bool = False) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Map trims using fuzzy matching

        Args:
            skip_special_brands: If True, include Mercedes/BMW in standard trim mapping
        """

        print(f"\n=== DEBUG: Inside _map_trims ===")
        print(f"Mercedes trims df: {len(mercedes_trims_df)} rows, {mercedes_trims_df['Input Trim'].nunique() if not mercedes_trims_df.empty and 'Input Trim' in mercedes_trims_df.columns else 0} unique")
        print(f"BMW trims df: {len(bmw_trims_df)} rows, {bmw_trims_df['Input Trim'].nunique() if not bmw_trims_df.empty and 'Input Trim' in bmw_trims_df.columns else 0} unique")

        # Build trim mapping dictionary - OPTIMIZED VERSION
        print("Building trim reference...")
        make_model_trims_grouped = processed_reference_df.groupby(
            [column_config.ref_make, column_config.ref_model]
        )[column_config.ref_trim].unique().apply(list).to_dict()

        # Convert tuple keys to string keys for compatibility
        make_model_trims = {f"{make}_{model}": trims
                           for (make, model), trims in make_model_trims_grouped.items()}

        print(f"Built trim reference: {len(make_model_trims)} make-model combinations")

        # Create input trims set (instead of dictionary to avoid overwriting duplicate trim strings with different make/model)
        if skip_special_brands:
            print(f"\n=== DEBUG: Filtering trims for standard mapping (INCLUDING Mercedes/BMW) ===")
        else:
            print(f"\n=== DEBUG: Filtering trims for standard mapping (EXCLUDING Mercedes/BMW) ===")
        input_trims_set = set() # Store combinations of (trim, make, model)
        dropped_trims = [] # Trims completely dropped due to parent missing
        total_rows = 0
        filtered_null_trim = 0
        filtered_null_make = 0
        filtered_null_model = 0
        filtered_mercedes = 0
        filtered_bmw = 0

        for _, row in tqdm(joined_input.iterrows(), total=len(joined_input), desc="Preparing input trims"):
            total_rows += 1
            input_trim_val = row[column_config.input_trim]

            # Track filtering reasons
            if pd.isna(input_trim_val):
                filtered_null_trim += 1
                continue
                
            # If make or model failed to map, the trim is dropped. Capture it for unmatched lists.
            if pd.isna(row['Mapped Make']):
                filtered_null_make += 1
                dropped_trims.append((input_trim_val, None, None))
                continue
            if pd.isna(row.get('Mapped Model', None)):
                filtered_null_model += 1
                dropped_trims.append((input_trim_val, row['Mapped Make'], None))
                continue

            # Only filter Mercedes/BMW when special processing is ENABLED
            if not skip_special_brands:
                if 'MERCEDES' in str(row['Mapped Make']).upper():
                    filtered_mercedes += 1
                    continue
                if 'BMW' in str(row['Mapped Make']).upper():
                    filtered_bmw += 1
                    continue

            # Add to input trims (as a tuple to guarantee uniqueness over combination of Trim + Make + Model)
            input_trims_set.add((input_trim_val, row['Mapped Make'], row['Mapped Model']))

        print(f"\n=== DEBUG: Trim filtering summary ===")
        print(f"Total rows processed: {total_rows}")
        print(f"Filtered out - null trim: {filtered_null_trim}")
        print(f"Filtered out - null make: {filtered_null_make}")
        print(f"Filtered out - null model: {filtered_null_model}")
        print(f"Filtered out - Mercedes: {filtered_mercedes}")
        print(f"Filtered out - BMW: {filtered_bmw}")
        print(f"Remaining for standard trim mapping: {len(input_trims_set)} unique combinations")

        # Create dropped trims dataframe
        dropped_trims_df = pd.DataFrame(dropped_trims, columns=['Input Trim', 'Master Make', 'Master Model'])
        dropped_trims_df['Best Match'] = None
        dropped_trims_df['Score'] = -1

        # Map standard trims using the unique combinations set
        mapped_trims_standard, unmatched_trims_standard = self._map_trims_standard(
            input_trims_set, make_model_trims, threshold, method)

        print(f"\n=== DEBUG: Standard trim mapping results ===")
        print(f"Standard mapped trims: {len(mapped_trims_standard)} rows, {mapped_trims_standard['Input Trim'].nunique() if not mapped_trims_standard.empty else 0} unique")
        print(f"Standard unmatched trims: {len(unmatched_trims_standard)} rows, {unmatched_trims_standard['Input Trim'].nunique() if not unmatched_trims_standard.empty else 0} unique")

        # Combine all trim mappings
        print(f"\n=== DEBUG: Combining trim mappings ===")
        print(f"Before concat - Standard: {len(mapped_trims_standard)}, Mercedes: {len(mercedes_trims_df)}, BMW: {len(bmw_trims_df)}")

        mapped_trims = pd.concat([mapped_trims_standard, mercedes_trims_df, bmw_trims_df],
                                 ignore_index=True)
                                 
        # Include dropped trims that had no matched parents
        unmatched_trims = pd.concat([unmatched_trims_standard, dropped_trims_df], ignore_index=True)
        # Deduplicate unmatched trims
        if not unmatched_trims.empty:
            unmatched_trims = unmatched_trims.drop_duplicates(subset=['Input Trim'])
            
            # Remove any unmatched trims that were actually successfully mapped in another row
            if not mapped_trims.empty:
                successful_trims = set(mapped_trims['Input Trim'].dropna())
                unmatched_trims = unmatched_trims[~unmatched_trims['Input Trim'].isin(successful_trims)]

        print(f"After concat - Total mapped trims: {len(mapped_trims)} rows, {mapped_trims['Input Trim'].nunique() if not mapped_trims.empty else 0} unique")
        print(f"Final unmatched trims: {len(unmatched_trims)} rows, {unmatched_trims['Input Trim'].nunique() if not unmatched_trims.empty else 0} unique")

        if not mapped_trims.empty:
            print(f"\nMapped trims columns: {list(mapped_trims.columns)}")
            print(f"Sample mapped trims:")
            print(mapped_trims.head(10))

        return mapped_trims, unmatched_trims

    def _map_trims_standard(self, input_trims_set: set,
                            make_model_trims: Dict[str, List[str]],
                            threshold: int = 80, method: str = "default") -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Map standard trims using fuzzy matching"""
        results = []
        unmatched = []
        scorer = self.get_scorer(method=method, simple=False)

        for trim, master_make, master_model in tqdm(input_trims_set, desc="Mapping Trims"):
            make_model_key = f"{master_make}_{master_model}"
            if make_model_key not in make_model_trims:
                unmatched.append((trim, None, -1, master_make, master_model))
                continue

            reference_trims = make_model_trims[make_model_key]
            input_value, matched_value, score = self.find_match_fuzzy(
                trim, reference_trims, threshold, simple=False, method=method)

            if matched_value is not None:
                results.append((input_value, matched_value,
                               score, master_make, master_model))
            else:
                results.append(
                    (input_value, None, -1, master_make, master_model))
                # Use the same scorer as the matching method
                best_match = process.extractOne(
                    trim, reference_trims, scorer=scorer)
                unmatched.append(
                    (trim, best_match[0] if best_match else None,
                     best_match[1] if best_match else -1, master_make, master_model))

        mapped_trims = pd.DataFrame(results, columns=[
            'Input Trim', 'Mapped Trim', 'Score', 'Master Make', 'Master Model'])
        unmatched_trims = pd.DataFrame(unmatched, columns=[
            'Input Trim', 'Best Match', 'Score', 'Master Make', 'Master Model'])

        return mapped_trims[mapped_trims['Mapped Trim'].notnull()], unmatched_trims

    def _process_mercedes_vehicles(self, mercedes_vehicles: pd.DataFrame,
                                   column_config: ColumnConfig, skip_trim: bool = False,
                                   mercedes_catalog: Optional[Dict[str, object]] = None) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Process Mercedes vehicles with special extraction logic"""
        mercedes_model_mappings = []
        mercedes_trim_mappings = []
        unmapped_mercedes = []

        for idx, row in mercedes_vehicles.iterrows():
            original_model = row[column_config.input_model]
            original_trim = row.get(column_config.input_trim, None) if column_config.input_trim else None

            # Clean the original model name BEFORE extraction
            cleaned_model = self.clean_extracted_model_name(original_model)

            extracted = self.extract_mercedes_class_and_trim(
                cleaned_model,
                include_trim=not skip_trim,
                input_trim=original_trim,
                catalog=mercedes_catalog
            )

            if extracted:
                if skip_trim:
                    mercedes_class = extracted
                    mercedes_trim = None
                else:
                    if ' | ' in extracted:
                        mercedes_class, mercedes_trim = extracted.split(
                            ' | ', 1)
                    else:
                        mercedes_class = extracted
                        mercedes_trim = None

                mercedes_vehicles.at[idx,
                                     column_config.input_model] = mercedes_class
                if not skip_trim and column_config.input_trim and mercedes_trim:
                    mercedes_vehicles.at[idx,
                                         column_config.input_trim] = mercedes_trim

                # Separate model mapping entry
                model_entry = {
                    'Input Model': original_model,
                    'Mapped Model': mercedes_class,
                    'Score': 100,
                    'Master Make': row.get('Mapped Make', 'MERCEDES-BENZ')
                }
                mercedes_model_mappings.append(model_entry)

                # Separate trim mapping entry (only if trim was extracted)
                # For Mercedes/BMW, trim is extracted FROM the model, so we key by Input Model
                if not skip_trim and mercedes_trim:
                    trim_entry = {
                        'Input Model': original_model,  # Use ORIGINAL MODEL as merge key
                        'Input Trim': original_trim,  # Keep for reference
                        'Extracted Trim': mercedes_trim,  # Store extracted trim for reference
                        'Mapped Trim': mercedes_trim,
                        'Score': 100,
                        'Master Make': row.get('Mapped Make', 'MERCEDES-BENZ'),
                        'Master Model': mercedes_class
                    }
                    mercedes_trim_mappings.append(trim_entry)
            else:
                # Track failed extractions
                unmapped_mercedes.append({
                    'Input Model': original_model,
                    'Cleaned Model': cleaned_model,
                    'Best Match': None,
                    'Score': -1,
                    'Master Make': row.get('Mapped Make', 'MERCEDES-BENZ'),
                    'Reason': 'Reference-based extraction failed'
                })

        mercedes_models_df = pd.DataFrame(mercedes_model_mappings)
        mercedes_trims_df = pd.DataFrame(mercedes_trim_mappings)
        unmapped_mercedes_df = pd.DataFrame(unmapped_mercedes)

        return mercedes_models_df, mercedes_trims_df, unmapped_mercedes_df

    def _process_bmw_vehicles(self, bmw_vehicles: pd.DataFrame,
                              column_config: ColumnConfig, skip_trim: bool = False,
                              bmw_catalog: Optional[Dict[str, object]] = None) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Process BMW vehicles with special extraction logic"""
        bmw_model_mappings = []
        bmw_trim_mappings = []
        unmapped_bmw = []

        for idx, row in bmw_vehicles.iterrows():
            original_model = row[column_config.input_model]
            original_trim = row.get(column_config.input_trim, None) if column_config.input_trim else None

            # Clean the original model name BEFORE extraction
            cleaned_model = self.clean_extracted_model_name(original_model)

            extracted = self.extract_bmw_series_and_trim(
                cleaned_model,
                include_trim=not skip_trim,
                input_trim=original_trim,
                catalog=bmw_catalog
            )

            if extracted:
                if skip_trim:
                    bmw_series = extracted
                    bmw_trim = None
                else:
                    if ' | ' in extracted:
                        bmw_series, bmw_trim = extracted.split(' | ', 1)
                    else:
                        bmw_series = extracted
                        bmw_trim = None

                bmw_vehicles.at[idx, column_config.input_model] = bmw_series
                if not skip_trim and column_config.input_trim and bmw_trim:
                    bmw_vehicles.at[idx, column_config.input_trim] = bmw_trim

                # Separate model mapping entry
                model_entry = {
                    'Input Model': original_model,
                    'Mapped Model': bmw_series,
                    'Score': 100,
                    'Master Make': row.get('Mapped Make', 'BMW')
                }
                bmw_model_mappings.append(model_entry)

                # Separate trim mapping entry (only if trim was extracted)
                # For Mercedes/BMW, trim is extracted FROM the model, so we key by Input Model
                if not skip_trim and bmw_trim:
                    trim_entry = {
                        'Input Model': original_model,  # Use ORIGINAL MODEL as merge key
                        'Input Trim': original_trim,  # Keep for reference
                        'Extracted Trim': bmw_trim,  # Store extracted trim for reference
                        'Mapped Trim': bmw_trim,
                        'Score': 100,
                        'Master Make': row.get('Mapped Make', 'BMW'),
                        'Master Model': bmw_series
                    }
                    bmw_trim_mappings.append(trim_entry)
            else:
                # Track failed extractions
                unmapped_bmw.append({
                    'Input Model': original_model,
                    'Cleaned Model': cleaned_model,
                    'Best Match': None,
                    'Score': -1,
                    'Master Make': row.get('Mapped Make', 'BMW'),
                    'Reason': 'Reference-based extraction failed'
                })

        bmw_models_df = pd.DataFrame(bmw_model_mappings)
        bmw_trims_df = pd.DataFrame(bmw_trim_mappings)
        unmapped_bmw_df = pd.DataFrame(unmapped_bmw)

        return bmw_models_df, bmw_trims_df, unmapped_bmw_df

    def save_results(self, results: Dict[str, pd.DataFrame], original_input_df: pd.DataFrame,
                     column_config: ColumnConfig, use_original_on_no_match: bool = False) -> Dict[str, io.BytesIO]:
        """Save mapping results to multiple files"""

        files = {}

        print("\n=== DEBUG: Starting save_results ===")
        print(f"Results keys: {results.keys()}")
        for key in results:
            if isinstance(results[key], pd.DataFrame):
                print(f"{key}: {len(results[key])} rows, {list(results[key].columns)}")
            else:
                print(f"{key}: type = {type(results[key])}")

        # Create mapped files
        print("\n=== DEBUG: Creating mapped files ===")
        for key in ['mapped_makes', 'mapped_models', 'mapped_trims']:
            try:
                if key in results and not results[key].empty:
                    print(f"Processing {key}...")
                    output = io.BytesIO()
                    results[key].to_csv(output, index=False)
                    output.seek(0)
                    files[f"{key}.csv"] = output
                    print(f"✓ {key}.csv created")
                else:
                    print(f"⊘ Skipping {key} (not in results or empty)")
            except Exception as e:
                print(f"✗ Error creating {key}.csv: {str(e)}")
                raise

        # Create unmatched files (create even if empty so users can see 0 unmatched items)
        print("\n=== DEBUG: Creating unmatched files ===")
        for key in ['unmatched_makes', 'unmatched_models', 'unmatched_trims']:
            try:
                if key in results:
                    print(f"Processing {key}...")
                    output = io.BytesIO()
                    results[key].to_csv(output, index=False)
                    output.seek(0)
                    files[f"{key}.csv"] = output
                    if results[key].empty:
                        print(f"✓ {key}.csv created (empty - all items matched!)")
                    else:
                        print(f"✓ {key}.csv created ({len(results[key])} unmatched items)")
                else:
                    print(f"⊘ Skipping {key} (not in results - dimension was skipped)")
            except Exception as e:
                print(f"✗ Error creating {key}.csv: {str(e)}")
                raise

        # Create Gemini verification log JSON if available
        if 'gemini_verification_log' in results and results['gemini_verification_log']:
            import json
            print("\n=== DEBUG: Creating Gemini verification log ===")
            log_data = results['gemini_verification_log']
            output = io.BytesIO()
            output.write(json.dumps(log_data, indent=2, ensure_ascii=False).encode('utf-8'))
            output.seek(0)
            files['gemini_verification_log.json'] = output
            print(f"✓ gemini_verification_log.json created ({len(log_data)} entries)")

        # Create consolidated file
        print("\n=== DEBUG: Creating consolidated file ===")
        print(f"Original input shape: {original_input_df.shape}")
        consolidated_df = original_input_df.copy()

        # Process original data to match sanitized values
        print("\n=== DEBUG: Processing original data ===")
        input_columns = {
            'make': column_config.input_make,
            'model': column_config.input_model,
            'trim': column_config.input_trim
        }
        processed_original_df = self.process_df(consolidated_df, input_columns)
        print(f"Processed original shape: {processed_original_df.shape}")

        # Add mapped columns
        try:
            if 'mapped_makes' in results:
                print("\n=== DEBUG: Merging mapped makes ===")
                print(f"Consolidated df shape before: {consolidated_df.shape}")
                print(f"Mapped makes shape: {results['mapped_makes'].shape}")
                print(f"Merging on column: {column_config.input_make}")

                # Create temporary column for merge key
                consolidated_df['_temp_make_key'] = processed_original_df[column_config.input_make]

                consolidated_df = consolidated_df.merge(
                    results['mapped_makes'][['Input Make', 'Mapped Make']],
                    left_on='_temp_make_key',
                    right_on='Input Make',
                    how='left'
                ).drop(columns=['Input Make', '_temp_make_key'])
                print(f"Consolidated df shape after: {consolidated_df.shape}")
                print("✓ Makes merged successfully")
        except Exception as e:
            print(f"✗ Error merging makes: {str(e)}")
            import traceback
            traceback.print_exc()
            raise

        try:
            if 'mapped_models' in results and not results['mapped_models'].empty:
                print("\n=== DEBUG: Merging mapped models ===")
                print(f"Consolidated df shape before: {consolidated_df.shape}")
                print(f"Mapped models shape: {results['mapped_models'].shape}")
                print(f"Mapped models columns: {list(results['mapped_models'].columns)}")

                # CRITICAL: Deduplicate mapped_models to avoid Cartesian product
                # Keep only unique Input Model -> Mapped Model mappings
                models_for_merge = results['mapped_models'][['Input Model', 'Mapped Model']].drop_duplicates(subset=['Input Model'])

                print(f"After deduplication: {len(models_for_merge)} unique models (was {len(results['mapped_models'])})")
                print(f"Merging on column: {column_config.input_model}")

                # Create temporary column for merge key
                consolidated_df['_temp_model_key'] = processed_original_df[column_config.input_model]

                consolidated_df = consolidated_df.merge(
                    models_for_merge,
                    left_on='_temp_model_key',
                    right_on='Input Model',
                    how='left'
                ).drop(columns=['Input Model', '_temp_model_key'])
                print(f"Consolidated df shape after: {consolidated_df.shape}")
                print(f"Null Mapped Model count: {consolidated_df['Mapped Model'].isna().sum()}")
                print("✓ Models merged successfully")
        except Exception as e:
            print(f"✗ Error merging models: {str(e)}")
            import traceback
            traceback.print_exc()
            raise

        try:
            if 'mapped_trims' in results and not results['mapped_trims'].empty:
                print("\n=== DEBUG: Merging mapped trims ===")
                print(f"Consolidated df shape before: {consolidated_df.shape}")
                print(f"Mapped trims shape: {results['mapped_trims'].shape}")
                print(f"Mapped trims columns: {list(results['mapped_trims'].columns)}")

                # Split trim mappings into two types:
                # 1. Standard trims (keyed by Make + Model + Trim)
                # 2. Mercedes/BMW trims (keyed by Make + Input Model)

                # Check if 'Input Model' column exists (only present when Mercedes/BMW trims exist)
                if 'Input Model' in results['mapped_trims'].columns:
                    standard_trims = results['mapped_trims'][~results['mapped_trims']['Input Model'].notna()]
                    special_trims = results['mapped_trims'][results['mapped_trims']['Input Model'].notna()]
                else:
                    # No Mercedes/BMW trims, all are standard
                    standard_trims = results['mapped_trims']
                    special_trims = pd.DataFrame()

                print(f"\nStandard trims: {len(standard_trims)}")
                print(f"Mercedes/BMW trims: {len(special_trims)}")

                # First, merge Mercedes/BMW trims using Make + Input Model
                if not special_trims.empty:
                    print("\n=== Merging Mercedes/BMW trims ===")
                    print(f"Total special trims: {len(special_trims)}")

                    # CRITICAL: Deduplicate special_trims to avoid Cartesian product!
                    special_trims_unique = special_trims[['Master Make', 'Input Model', 'Mapped Trim']].drop_duplicates(subset=['Master Make', 'Input Model'])
                    print(f"After deduplication: {len(special_trims_unique)} unique (was {len(special_trims)})")

                    print(f"Sample special trims:")
                    print(special_trims_unique.head(5))

                    # Create merge key using processed original model
                    consolidated_df['_temp_make_key'] = consolidated_df.get('Mapped Make', processed_original_df[column_config.input_make])
                    consolidated_df['_temp_model_key'] = processed_original_df[column_config.input_model]

                    print(f"Sample merge keys:")
                    print(f"  Make: {consolidated_df['_temp_make_key'].head(5).tolist()}")
                    print(f"  Model: {consolidated_df['_temp_model_key'].head(5).tolist()}")

                    consolidated_df = consolidated_df.merge(
                        special_trims_unique,
                        left_on=['_temp_make_key', '_temp_model_key'],
                        right_on=['Master Make', 'Input Model'],
                        how='left',
                        suffixes=('', '_special')
                    ).drop(columns=['Master Make', 'Input Model', '_temp_make_key', '_temp_model_key'])

                    # Rename the merged column
                    if 'Mapped Trim_special' in consolidated_df.columns:
                        consolidated_df['Mapped Trim'] = consolidated_df['Mapped Trim_special']
                        consolidated_df = consolidated_df.drop(columns=['Mapped Trim_special'])

                    print(f"After special trims merge: {consolidated_df['Mapped Trim'].notna().sum()} mapped")

                # Then, merge standard trims using Make + Model + Trim
                if not standard_trims.empty:
                    print("\n=== Merging standard trims ===")
                    print(f"Standard trims to merge: {len(standard_trims)}")
                    print(f"Sample standard trims:")
                    print(standard_trims[['Master Make', 'Master Model', 'Input Trim', 'Mapped Trim']].head(5))

                    # Create merge keys - use Mapped Make/Model if they exist, otherwise use processed original
                    if 'Mapped Make' in consolidated_df.columns:
                        consolidated_df['_temp_make_for_trim'] = consolidated_df['Mapped Make'].fillna(processed_original_df[column_config.input_make])
                    else:
                        consolidated_df['_temp_make_for_trim'] = processed_original_df[column_config.input_make]

                    if 'Mapped Model' in consolidated_df.columns:
                        consolidated_df['_temp_model_for_trim'] = consolidated_df['Mapped Model'].fillna(processed_original_df[column_config.input_model])
                    else:
                        consolidated_df['_temp_model_for_trim'] = processed_original_df[column_config.input_model]

                    consolidated_df['_temp_trim_key'] = processed_original_df[column_config.input_trim]

                    print(f"\nSample merge keys from consolidated (first 5 non-null model rows):")
                    sample_mask = consolidated_df['_temp_model_for_trim'].notna()
                    sample_rows = consolidated_df[sample_mask].head(5)
                    print(f"  Make: {sample_rows['_temp_make_for_trim'].tolist()}")
                    print(f"  Model: {sample_rows['_temp_model_for_trim'].tolist()}")
                    print(f"  Trim: {sample_rows['_temp_trim_key'].tolist()}")

                    # Check if we have CHEVROLET CAPRICE specifically
                    chevrolet_caprice = consolidated_df[
                        (consolidated_df['_temp_make_for_trim'] == 'CHEVROLET') &
                        (consolidated_df['_temp_model_for_trim'] == 'CAPRICE')
                    ]
                    print(f"\nCHEVROLET CAPRICE rows in consolidated: {len(chevrolet_caprice)}")
                    if not chevrolet_caprice.empty:
                        print(f"Sample CHEVROLET CAPRICE trim keys: {chevrolet_caprice['_temp_trim_key'].head(5).tolist()}")

                    # Check what's in standard_trims for CHEVROLET CAPRICE
                    chevrolet_caprice_trims = standard_trims[
                        (standard_trims['Master Make'] == 'CHEVROLET') &
                        (standard_trims['Master Model'] == 'CAPRICE')
                    ]
                    print(f"CHEVROLET CAPRICE rows in standard_trims: {len(chevrolet_caprice_trims)}")
                    if not chevrolet_caprice_trims.empty:
                        print(f"Sample CHEVROLET CAPRICE Input Trims: {chevrolet_caprice_trims['Input Trim'].tolist()}")

                    # Only merge where Mapped Trim is still null (not already filled by special trims)
                    mask_null_trim = consolidated_df['Mapped Trim'].isna() if 'Mapped Trim' in consolidated_df.columns else pd.Series([True] * len(consolidated_df))
                    print(f"\nRows with null Mapped Trim (eligible for standard merge): {mask_null_trim.sum()}")

                    # Merge ALL rows (not just null trim rows) to avoid index mismatch
                    consolidated_df = consolidated_df.merge(
                        standard_trims[['Master Make', 'Master Model', 'Input Trim', 'Mapped Trim']],
                        left_on=['_temp_make_for_trim', '_temp_model_for_trim', '_temp_trim_key'],
                        right_on=['Master Make', 'Master Model', 'Input Trim'],
                        how='left',
                        suffixes=('', '_std')
                    )

                    print(f"consolidated_df shape after merge: {consolidated_df.shape}")
                    print(f"consolidated_df columns: {list(consolidated_df.columns)}")

                    # Update Mapped Trim ONLY where it was null and standard trim matched
                    if 'Mapped Trim_std' in consolidated_df.columns:
                        # Fill null Mapped Trim values with standard matches
                        update_mask = mask_null_trim & consolidated_df['Mapped Trim_std'].notna()
                        matched_count = update_mask.sum()
                        print(f"Matched standard trims: {matched_count}")
                        if matched_count > 0:
                            consolidated_df.loc[update_mask, 'Mapped Trim'] = consolidated_df.loc[update_mask, 'Mapped Trim_std']
                        # Drop the _std suffix column
                        consolidated_df = consolidated_df.drop(columns=['Mapped Trim_std'])
                    elif 'Mapped Trim' in consolidated_df.columns and 'Mapped Trim' not in ['Mapped Make', 'Mapped Model']:
                        # First merge, Mapped Trim column exists from standard merge
                        matched_count = consolidated_df['Mapped Trim'].notna().sum()
                        print(f"Matched standard trims (first merge): {matched_count}")

                    # Drop the merge key columns from right side
                    consolidated_df = consolidated_df.drop(columns=['Master Make', 'Master Model', 'Input Trim'], errors='ignore')

                    # Show sample matched trims
                    if 'Mapped Trim' in consolidated_df.columns:
                        sample_matched = consolidated_df[consolidated_df['Mapped Trim'].notna() & ~consolidated_df['Mapped Trim'].isin(['16I', '18', '20', '200', '25', '28'])].head(5)
                        if not sample_matched.empty:
                            print(f"Sample newly matched standard trims:")
                            print(sample_matched[[column_config.input_make, column_config.input_model, column_config.input_trim, 'Mapped Trim']])

                    consolidated_df = consolidated_df.drop(columns=['_temp_make_for_trim', '_temp_model_for_trim', '_temp_trim_key'], errors='ignore')

                    print(f"After standard trims merge: {consolidated_df['Mapped Trim'].notna().sum()} total mapped")

                print(f"\n=== Final trim merge results ===")
                print(f"Consolidated df shape after: {consolidated_df.shape}")
                print(f"Mapped Trim column null count: {consolidated_df['Mapped Trim'].isna().sum()}")
                print(f"Mapped Trim column non-null count: {consolidated_df['Mapped Trim'].notna().sum()}")
                if 'Mapped Trim' in consolidated_df.columns and consolidated_df['Mapped Trim'].notna().sum() > 0:
                    print(f"Sample mapped trim values: {consolidated_df[consolidated_df['Mapped Trim'].notna()]['Mapped Trim'].head(10).tolist()}")
                print("✓ Trims merged successfully")
        except Exception as e:
            print(f"✗ Error merging trims: {str(e)}")
            import traceback
            traceback.print_exc()
            raise

        # Handle unmapped values based on toggle option
        print("\n=== DEBUG: Handling unmapped values ===")
        try:
            if use_original_on_no_match:
                print("Using original values for unmapped records")
                # Use original values instead of NaN for unmapped records
                if 'Mapped Make' in consolidated_df.columns:
                    print(f"Filling NaN in Mapped Make column")
                    consolidated_df['Mapped Make'] = consolidated_df['Mapped Make'].fillna(
                        consolidated_df[column_config.input_make])

                if 'Mapped Model' in consolidated_df.columns:
                    print(f"Filling NaN in Mapped Model column")
                    consolidated_df['Mapped Model'] = consolidated_df['Mapped Model'].fillna(
                        consolidated_df[column_config.input_model])

                if 'Mapped Trim' in consolidated_df.columns and column_config.input_trim:
                    print(f"Filling NaN in Mapped Trim column")
                    consolidated_df['Mapped Trim'] = consolidated_df['Mapped Trim'].fillna(
                        consolidated_df[column_config.input_trim])
                print("✓ Unmapped values handled")
            else:
                print("Keeping NaN for unmapped records")
        except Exception as e:
            print(f"✗ Error handling unmapped values: {str(e)}")
            import traceback
            traceback.print_exc()
            raise

        # Save consolidated file
        print("\n=== DEBUG: Saving consolidated file ===")
        try:
            print(f"Final consolidated df shape: {consolidated_df.shape}")
            print(f"Columns: {list(consolidated_df.columns)}")
            output = io.BytesIO()
            consolidated_df.to_csv(output, index=False)
            output.seek(0)
            files['consolidated_data.csv'] = output
            print("✓ Consolidated file saved")
        except Exception as e:
            print(f"✗ Error saving consolidated file: {str(e)}")
            import traceback
            traceback.print_exc()
            raise

        # Create summary
        print("\n=== DEBUG: Creating summary ===")
        try:
            # Calculate statistics safely
            print("Calculating makes statistics...")
            mapped_makes_count = 0
            unmatched_makes_count = 0
            if 'mapped_makes' in results and isinstance(results['mapped_makes'], pd.DataFrame) and 'Input Make' in results['mapped_makes'].columns:
                mapped_makes_count = results['mapped_makes']['Input Make'].nunique()
                print(f"  Mapped makes: {mapped_makes_count}")
            if 'unmatched_makes' in results and isinstance(results['unmatched_makes'], pd.DataFrame) and 'Input Make' in results['unmatched_makes'].columns:
                unmatched_makes_count = results['unmatched_makes']['Input Make'].nunique()
                print(f"  Unmatched makes: {unmatched_makes_count}")

            print("Calculating models statistics...")
            mapped_models_count = 0
            unmatched_models_count = 0
            if 'mapped_models' in results and isinstance(results['mapped_models'], pd.DataFrame) and 'Input Model' in results['mapped_models'].columns:
                mapped_models_count = results['mapped_models']['Input Model'].nunique()
                print(f"  Mapped models: {mapped_models_count}")
            if 'unmatched_models' in results and isinstance(results['unmatched_models'], pd.DataFrame) and 'Input Model' in results['unmatched_models'].columns:
                unmatched_models_count = results['unmatched_models']['Input Model'].nunique()
                print(f"  Unmatched models: {unmatched_models_count}")

            print("Calculating trims statistics...")
            mapped_trims_count = 0
            unmatched_trims_count = 0
            if 'mapped_trims' in results and isinstance(results['mapped_trims'], pd.DataFrame) and 'Input Trim' in results['mapped_trims'].columns:
                mapped_trims_df = results['mapped_trims']
                print(f"  Mapped trims DataFrame shape: {mapped_trims_df.shape}")
                print(f"  Mapped trims columns: {list(mapped_trims_df.columns)}")
                print(f"  Total mapped trim rows: {len(mapped_trims_df)}")

                # Show value counts
                trim_value_counts = mapped_trims_df['Input Trim'].value_counts()
                print(f"  Unique trim values: {len(trim_value_counts)}")
                print(f"  Top 10 most common trims:")
                print(trim_value_counts.head(10))

                mapped_trims_count = mapped_trims_df['Input Trim'].nunique()
                print(f"  SUMMARY: Mapped trims (unique): {mapped_trims_count}")
            else:
                print(f"  No mapped_trims in results or missing 'Input Trim' column")

            if 'unmatched_trims' in results and isinstance(results['unmatched_trims'], pd.DataFrame) and 'Input Trim' in results['unmatched_trims'].columns:
                unmatched_trims_df = results['unmatched_trims']
                print(f"  Unmatched trims DataFrame shape: {unmatched_trims_df.shape}")
                print(f"  Total unmatched trim rows: {len(unmatched_trims_df)}")

                # Show value counts
                trim_value_counts = unmatched_trims_df['Input Trim'].value_counts()
                print(f"  Unique trim values: {len(trim_value_counts)}")
                print(f"  Top 10 most common unmatched trims:")
                print(trim_value_counts.head(10))

                unmatched_trims_count = unmatched_trims_df['Input Trim'].nunique()
                print(f"  SUMMARY: Unmatched trims (unique): {unmatched_trims_count}")
            else:
                print(f"  No unmatched_trims in results or missing 'Input Trim' column")

            summary_data = {
                'Dimension': ['Makes', 'Models', 'Trims'],
                'Total': [
                    original_input_df[column_config.input_make].dropna().nunique() if column_config.input_make in original_input_df.columns else 0,
                    original_input_df[column_config.input_model].dropna().nunique() if column_config.input_model in original_input_df.columns else 0,
                    original_input_df[column_config.input_trim].dropna().nunique() if column_config.input_trim in original_input_df.columns else 0
                ],
                'Mapped': [
                    mapped_makes_count,
                    mapped_models_count,
                    mapped_trims_count
                ],
                'Unmatched': [
                    unmatched_makes_count,
                    unmatched_models_count,
                    unmatched_trims_count
                ]
            }

            print("Creating summary dataframe...")
            summary_df = pd.DataFrame(summary_data)
            print(f"Summary:\n{summary_df}")

            print("Saving summary file...")
            output = io.BytesIO()
            summary_df.to_csv(output, index=False)
            output.seek(0)
            files['mapping_summary.csv'] = output
            print("✓ Summary file saved")
        except Exception as e:
            print(f"✗ Error creating summary: {str(e)}")
            import traceback
            traceback.print_exc()
            raise

        print("\n=== DEBUG: save_results completed successfully ===")
        print(f"Total files created: {len(files)}")
        print(f"Files: {list(files.keys())}")
        return files
