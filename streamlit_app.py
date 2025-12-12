import streamlit as st
import pandas as pd
import io
import os
import asyncio
from typing import Dict, List, Optional, Any
import zipfile
import time

# Import workflow modules
from scrips.workflows.discrepancy_checker import DiscrepancyChecker
from scrips.workflows.discrepancy_analyzer import DiscrepancyAnalyzer
from scrips.workflows.spec_mapper import SpecMapper, ColumnConfig
from scrips.workflows.vehicle_verifier import VehicleDataVerifier, TRANSLATION_AVAILABLE
from scrips.workflows.old_new_validator import OldNewDataValidator
from scrips.workflows.combined_verifier_checker import CombinedVerifierChecker

# Import translation service
try:
    from scrips.services.translation_service import ArabicTranslationService
    TRANSLATION_SERVICE_AVAILABLE = True
except ImportError:
    TRANSLATION_SERVICE_AVAILABLE = False

# Configure page
st.set_page_config(
    page_title="Vehicle Data Analysis Suite",
    page_icon="🚗",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Add custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(90deg, #4CAF50 0%, #45a049 100%);
        color: white;
        padding: 20px;
        border-radius: 10px;
        text-align: center;
        margin-bottom: 30px;
    }
    
    .workflow-card {
        background: #f8f9fa;
        border: 1px solid #dee2e6;
        border-radius: 8px;
        padding: 20px;
        margin: 10px 0;
        word-wrap: break-word;
        overflow-wrap: break-word;
        white-space: normal;
        max-width: 100%;
    }
    
    .workflow-card h3 {
        margin-top: 0;
        margin-bottom: 15px;
        color: #495057;
        font-size: 1.5rem;
        line-height: 1.4;
    }
    
    .workflow-card p {
        margin-bottom: 0;
        color: #6c757d;
        font-size: 1rem;
        line-height: 1.6;
        word-wrap: break-word;
        overflow-wrap: break-word;
        white-space: normal;
    }
    
    .success-message {
        background: #d4edda;
        border: 1px solid #c3e6cb;
        color: #155724;
        padding: 15px;
        border-radius: 5px;
        margin: 10px 0;
    }
    
    .error-message {
        background: #f8d7da;
        border: 1px solid #f5c6cb;
        color: #721c24;
        padding: 15px;
        border-radius: 5px;
        margin: 10px 0;
    }
    
    .info-message {
        background: #d1ecf1;
        border: 1px solid #bee5eb;
        color: #0c5460;
        padding: 15px;
        border-radius: 5px;
        margin: 10px 0;
    }
    
    /* Fix any potential text overflow issues */
    .stMarkdown, .stMarkdown div {
        word-wrap: break-word !important;
        overflow-wrap: break-word !important;
        white-space: normal !important;
    }
    
    /* Ensure proper text display in all containers */
    div[data-testid="stMarkdownContainer"] {
        word-wrap: break-word !important;
        overflow-wrap: break-word !important;
        white-space: normal !important;
        max-width: 100% !important;
    }
</style>
""", unsafe_allow_html=True)


def safe_display_dataframe(df: pd.DataFrame, max_rows: int = 5, title: str = "Data Preview"):
    """
    Safely display a DataFrame with multiple fallback options for PyArrow issues.

    Args:
        df: DataFrame to display
        max_rows: Maximum number of rows to show
        title: Title for the display section
    """
    if df is None or df.empty:
        st.write("No data to display")
        return

    try:
        # First attempt: Use cleaned dataframe
        display_df = clean_dataframe_for_display(df, max_rows)
        st.dataframe(display_df)
    except Exception as e1:
        try:
            # Second attempt: Convert everything to string and try again
            display_df = df.head(max_rows).copy()
            for col in display_df.columns:
                display_df[col] = display_df[col].astype(str)
            st.dataframe(display_df)
        except Exception as e2:
            try:
                # Third attempt: Use st.table instead of st.dataframe
                display_df = df.head(max_rows).copy()
                for col in display_df.columns:
                    display_df[col] = display_df[col].astype(str)
                st.table(display_df)
            except Exception as e3:
                # Final fallback: Show as text
                st.write(
                    f"**{title}:** (Displaying as text due to formatting issues)")
                st.text(str(df.head(max_rows).to_string()))


def clean_dataframe_for_display(df: pd.DataFrame, max_rows: int = 5) -> pd.DataFrame:
    """
    Clean DataFrame for Streamlit display to avoid PyArrow conversion errors.

    Args:
        df: DataFrame to clean
        max_rows: Maximum number of rows to return

    Returns:
        Cleaned DataFrame safe for Streamlit display
    """
    if df is None or df.empty:
        return pd.DataFrame()

    # Get subset of data
    display_df = df.head(max_rows).copy()

    # Clean each column more aggressively
    for col in display_df.columns:
        try:
            # Convert all columns to string first to avoid any type issues
            display_df[col] = display_df[col].astype(str)

            # Replace problematic values
            display_df[col] = display_df[col].replace([
                'nan', 'None', 'NaT', 'nat', '<NA>', 'null', 'NULL',
                'NaN', 'NONE', 'Null'
            ], 'N/A')

            # Handle empty strings
            display_df[col] = display_df[col].replace('', 'N/A')

            # Truncate very long strings
            display_df[col] = display_df[col].apply(
                lambda x: str(x)[:100] + '...' if len(str(x)) > 100 else str(x)
            )

            # Remove any non-printable characters that might cause issues
            display_df[col] = display_df[col].apply(
                lambda x: ''.join(char for char in str(
                    x) if char.isprintable() or char.isspace())
            )

        except Exception as e:
            # If there's any issue with a column, make it a simple string
            display_df[col] = 'Data Preview Error'

    return display_df


def clean_dataframe_for_processing(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean DataFrame immediately after loading to prevent any PyArrow issues.

    Args:
        df: DataFrame to clean

    Returns:
        Cleaned DataFrame safe for processing
    """
    if df is None or df.empty:
        return df

    cleaned_df = df.copy()

    # Clean each column
    for col in cleaned_df.columns:
        try:
            # If column has mixed types or complex objects, convert to string
            if cleaned_df[col].dtype == 'object':
                # Check if column has problematic values
                sample_values = cleaned_df[col].dropna().head(10)
                has_complex_types = any(
                    isinstance(val, (dict, list, tuple, set)) or
                    str(type(val)).startswith('<class') for val in sample_values
                )

                if has_complex_types:
                    # Convert complex types to string representation
                    cleaned_df[col] = cleaned_df[col].apply(
                        lambda x: str(x) if pd.notna(x) else 'N/A'
                    )
                else:
                    # Convert to string and handle NaN
                    cleaned_df[col] = cleaned_df[col].astype(str)
                    cleaned_df[col] = cleaned_df[col].replace('nan', None)

        except Exception as e:
            # If there's any issue, convert entire column to string
            cleaned_df[col] = cleaned_df[col].astype(str)

    return cleaned_df


def display_header():
    """Display the main header"""
    st.markdown("""
    <div class="main-header">
        <h1>🚗 Vehicle Data Analysis Suite</h1>
        <p>Comprehensive tools for vehicle data processing, analysis, and verification</p>
    </div>
    """, unsafe_allow_html=True)


def get_file_type(file):
    """Get file type from uploaded file"""
    if file.name.endswith('.xlsx'):
        return 'xlsx'
    elif file.name.endswith('.xls'):
        return 'xls'
    elif file.name.endswith('.csv'):
        return 'csv'
    else:
        return 'unknown'


def create_download_link(data: io.BytesIO, filename: str, link_text: str):
    """Create a download link for data"""
    st.download_button(
        label=link_text,
        data=data,
        file_name=filename,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" if filename.endswith(
            '.xlsx') else "text/csv"
    )


def create_zip_download(files: Dict[str, io.BytesIO], zip_name: str):
    """Create a zip file download for multiple files"""
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        for filename, file_data in files.items():
            zip_file.writestr(filename, file_data.getvalue())

    zip_buffer.seek(0)

    st.download_button(
        label=f"📦 Download All Files ({zip_name})",
        data=zip_buffer,
        file_name=f"{zip_name}.zip",
        mime="application/zip"
    )


def discrepancy_checker_tab():
    """Discrepancy Checker workflow tab"""
    st.markdown("""
    <div class="workflow-card">
        <h3>📊 Discrepancy Checker</h3>
        <p>Excel/CSV processor with conditional formatting for discrepancy checking. 
        Compare two columns within a percentage threshold or check if values fall within a range.</p>
    </div>
    """, unsafe_allow_html=True)

    # Mode selection
    st.subheader("📋 File Mode")
    file_mode = st.radio(
        "Choose file mode:",
        ["Single file (all columns from one file)",
         "Two files (join data from separate files)"],
        key="dc_file_mode"
    )

    single_file_mode = file_mode.startswith("Single file")

    # File uploads
    if single_file_mode:
        st.subheader("📄 Data File")
        uploaded_file = st.file_uploader(
            "Upload Excel or CSV file",
            type=['xlsx', 'xls', 'csv'],
            key="discrepancy_checker_file"
        )
        join_file = None
    else:
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("📄 Primary Data File")
            uploaded_file = st.file_uploader(
                "Upload primary data file (Excel/CSV)",
                type=['xlsx', 'xls', 'csv'],
                key="dc_primary_file"
            )
        with col2:
            st.subheader("📄 Join Data File")
            join_file = st.file_uploader(
                "Upload join data file (Excel/CSV)",
                type=['xlsx', 'xls', 'csv'],
                key="dc_join_file"
            )

    # Process files based on mode
    if single_file_mode and uploaded_file is not None:
        file_type = get_file_type(uploaded_file)

        if file_type == 'unknown':
            st.error("❌ Unsupported file type. Please upload Excel (.xlsx, .xls) or CSV files.")
            return

        # Initialize checker
        checker = DiscrepancyChecker()

        try:
            # Load data and get sheets
            file_data = io.BytesIO(uploaded_file.read())
            sheets = checker.load_data(file_data, file_type)

            # Sheet selection
            if len(sheets) > 1:
                selected_sheet = st.selectbox("Select sheet:", sheets, key="dc_sheet")
            else:
                selected_sheet = sheets[0]

            # Load selected sheet
            file_data.seek(0)
            df = checker.select_sheet(selected_sheet, file_data, file_type)

            # Clean the DataFrame immediately after loading to prevent PyArrow issues
            df = clean_dataframe_for_processing(df)
            # Update the checker's internal dataframe with the cleaned version
            checker.df = df

            st.success(f"✅ Loaded {len(df)} records from sheet '{selected_sheet}'")
            st.write("**Data Preview:**")
            safe_display_dataframe(df, 5, "Data Preview")

        except Exception as e:
            st.error(f"❌ Error loading file: {str(e)}")
            st.info("💡 Try converting your file to CSV format if the error persists.")
            return
            
        columns = checker.get_columns()

    elif not single_file_mode and uploaded_file is not None and join_file is not None:
        # Create unique key for current files to detect changes
        current_files_key = f"{uploaded_file.name}_{join_file.name}_{uploaded_file.size}_{join_file.size}"
        
        # Initialize or reset checker when files change
        if 'dc_checker' not in st.session_state or st.session_state.get('dc_files_key') != current_files_key:
            st.session_state.dc_checker = DiscrepancyChecker()
            st.session_state.dc_files_key = current_files_key
            st.session_state.dc_data_joined = False
            
        checker = st.session_state.dc_checker

        try:
            # Only load data if not already loaded
            if not hasattr(checker, 'primary_df') or checker.primary_df is None:
                # Load primary data
                primary_type = get_file_type(uploaded_file)
                primary_data = io.BytesIO(uploaded_file.read())
                primary_sheets = checker.load_primary_data(primary_data, primary_type)

                # Load join data
                join_type = get_file_type(join_file)
                join_data = io.BytesIO(join_file.read())
                join_sheets = checker.load_join_data(join_data, join_type)

                # Store sheet info in session state
                st.session_state.dc_primary_sheets = primary_sheets
                st.session_state.dc_join_sheets = join_sheets
                st.session_state.dc_primary_data = primary_data
                st.session_state.dc_join_data = join_data
                st.session_state.dc_primary_type = primary_type
                st.session_state.dc_join_type = join_type
            else:
                # Use cached data
                primary_sheets = st.session_state.dc_primary_sheets
                join_sheets = st.session_state.dc_join_sheets

            # Sheet selection
            col1, col2 = st.columns(2)
            with col1:
                if len(primary_sheets) > 1:
                    primary_sheet = st.selectbox("Select primary data sheet:", primary_sheets, key="dc_primary_sheet")
                else:
                    primary_sheet = primary_sheets[0]
                    st.info(f"Using sheet: {primary_sheet}")

            with col2:
                if len(join_sheets) > 1:
                    join_sheet = st.selectbox("Select join data sheet:", join_sheets, key="dc_join_sheet")
                else:
                    join_sheet = join_sheets[0]
                    st.info(f"Using sheet: {join_sheet}")

            # Load selected sheets only if not already loaded or sheet changed
            if (not hasattr(checker, 'primary_df') or checker.primary_df is None or
                st.session_state.get('dc_selected_primary_sheet') != primary_sheet or
                st.session_state.get('dc_selected_join_sheet') != join_sheet):
                
                # Load selected sheets
                primary_data = st.session_state.dc_primary_data
                join_data = st.session_state.dc_join_data
                primary_type = st.session_state.dc_primary_type
                join_type = st.session_state.dc_join_type
                
                primary_data.seek(0)
                primary_df = checker.select_primary_sheet(primary_sheet, primary_data, primary_type)
                join_data.seek(0)
                join_df = checker.select_join_sheet(join_sheet, join_data, join_type)

                # Clean DataFrames immediately after loading to prevent PyArrow issues
                primary_df = clean_dataframe_for_processing(primary_df)
                join_df = clean_dataframe_for_processing(join_df)
                # Update checker's internal dataframes
                checker.primary_df = primary_df
                checker.join_df = join_df
                
                # Store selected sheets
                st.session_state.dc_selected_primary_sheet = primary_sheet
                st.session_state.dc_selected_join_sheet = join_sheet
                # Reset join status if sheets changed
                st.session_state.dc_data_joined = False

            st.success(f"✅ Loaded {len(checker.primary_df)} primary records and {len(checker.join_df)} join records")

            # Show join configuration
            st.subheader("🔗 Join Configuration")
            
            primary_columns = checker.get_primary_columns()
            join_columns = checker.get_join_columns()

            col1, col2, col3 = st.columns(3)
            with col1:
                primary_chassis_col = st.selectbox("Primary chassis column:", primary_columns, key="dc_primary_chassis")
            with col2:
                join_chassis_col = st.selectbox("Join chassis column:", join_columns, key="dc_join_chassis")
            with col3:
                join_type = st.selectbox("Join type:", ['left', 'inner', 'outer', 'right'], key="dc_join_type", index=0)

            # Check if join parameters changed
            current_join_params = f"{primary_chassis_col}_{join_chassis_col}_{join_type}"
            if st.session_state.get('dc_join_params') != current_join_params:
                st.session_state.dc_data_joined = False
                st.session_state.dc_join_params = current_join_params

            # Show join button only if data not joined
            if not st.session_state.dc_data_joined:
                if st.button("🔗 Join Data", key="dc_join_button"):
                    with st.spinner("Joining data..."):
                        try:
                            merged_df = checker.join_data(primary_chassis_col, join_chassis_col, join_type)
                            st.session_state.dc_data_joined = True
                            st.success(f"✅ Successfully joined data! Merged DataFrame has {len(merged_df)} records")
                            st.write("**Merged Data Preview:**")
                            safe_display_dataframe(merged_df, 5, "Merged Data Preview")
                            
                            # Show join statistics
                            col1, col2, col3 = st.columns(3)
                            with col1:
                                st.metric("Primary Records", len(checker.primary_df))
                            with col2:
                                st.metric("Join Records", len(checker.join_df))
                            with col3:
                                st.metric("Merged Records", len(merged_df))
                            
                            # Force rerun to show the analysis section
                            st.rerun()
                                
                        except Exception as e:
                            st.error(f"❌ Error joining data: {str(e)}")
                            return
                            
                # Don't show analysis section yet
                st.info("👆 Please join the data first before proceeding with analysis.")
                return

            # Show join status if data was already joined
            else:
                st.success(f"✅ Data already joined! Merged DataFrame has {len(checker.merged_df)} records")
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Primary Records", len(checker.primary_df))
                with col2:
                    st.metric("Join Records", len(checker.join_df))
                with col3:
                    st.metric("Merged Records", len(checker.merged_df))
                    
                # Show option to rejoin with different parameters
                if st.button("🔄 Rejoin Data", key="dc_rejoin_button"):
                    st.session_state.dc_data_joined = False
                    st.rerun()

            columns = checker.get_merged_columns()

        except Exception as e:
            st.error(f"❌ Error loading files: {str(e)}")
            return

    elif not single_file_mode:
        st.info("Please upload both primary and join data files to proceed.")
        return
    else:
        st.info("Please upload a data file to proceed.")
        return

    # Column selection and mode (common for both single and two-file modes)
    st.subheader("📋 Configuration")

    # Mode selection
    mode = st.radio(
        "Select analysis mode:",
        [
            "Compare two columns within % threshold",
            "Absolute % difference with color coding",
            "Check value between low & high columns"
        ],
        key="dc_mode"
    )

    if mode == "Compare two columns within % threshold":
        col1, col2 = st.columns(2)
        with col1:
            val_col1 = st.selectbox(
                "First value column:", columns, key="dc_val1")
        with col2:
            val_col2 = st.selectbox(
                "Second value column:", columns, key="dc_val2")

        threshold = st.number_input("Threshold percentage (e.g., 15 for 15%):",
                                    min_value=0.0, max_value=100.0, value=15.0, key="dc_threshold")

        # Dividend selection
        st.subheader("📊 Calculation Settings")
        dividend_choice = st.radio(
            "Which column to use as dividend (denominator):",
            ["First column", "Second column"],
            index=1,  # Default to Second column (current behavior)
            key="dc_dividend_choice",
            help="The dividend is the column used as the denominator in percentage calculations"
        )
        
        # Show calculation formula
        if dividend_choice == "First column":
            st.info(f"📊 **Calculation**: ((Second - First) / First) × 100 = (({val_col2} - {val_col1}) / {val_col1}) × 100")
        else:
            st.info(f"📊 **Calculation**: ((First - Second) / Second) × 100 = (({val_col1} - {val_col2}) / {val_col2}) × 100")

        # Highlighting mode selection
        highlight_mode = st.radio(
            "Highlighting mode:",
            ["Entire row", "Percentage difference column only"],
            key="dc_highlight_mode"
        )
        highlight_mode_value = "entire_row" if highlight_mode == "Entire row" else "percentage_column"

        if st.button("🔍 Process Analysis", key="dc_process"):
            with st.spinner("Processing..."):
                try:
                    # Determine dividend column based on user selection
                    dividend_col = val_col1 if dividend_choice == "First column" else val_col2
                    
                    result = checker.process_comparison_mode(
                        val_col1, val_col2, threshold, highlight_mode_value, dividend_col)
                    st.success("✅ Analysis completed!")

                    # Dynamic filename based on mode
                    filename = f"discrepancy_analysis_{'merged' if not single_file_mode else 'single'}.xlsx"
                    create_download_link(
                        result,
                        filename,
                        "📥 Download Analysis Results"
                    )
                except Exception as e:
                    st.error(f"❌ Error during processing: {str(e)}")

    elif mode == "Absolute % difference with color coding":
        st.info("🎨 **Color coding:** Faint red for values above threshold, faint blue for values below threshold")
        
        col1, col2 = st.columns(2)
        with col1:
            val_col1 = st.selectbox(
                "First value column:", columns, key="dc_abs_val1")
        with col2:
            val_col2 = st.selectbox(
                "Second value column:", columns, key="dc_abs_val2")

        threshold = st.number_input("Threshold percentage (e.g., 15 for 15%):",
                                    min_value=0.0, max_value=100.0, value=15.0, key="dc_abs_threshold")

        # Dividend selection
        st.subheader("📊 Calculation Settings")
        dividend_choice_abs = st.radio(
            "Which column to use as dividend (denominator):",
            ["First column", "Second column"],
            index=1,  # Default to Second column (current behavior)
            key="dc_abs_dividend_choice",
            help="The dividend is the column used as the denominator in percentage calculations"
        )
        
        # Show calculation formula
        if dividend_choice_abs == "First column":
            st.info(f"📊 **Calculation**: ((Second - First) / First) × 100 = (({val_col2} - {val_col1}) / {val_col1}) × 100")
        else:
            st.info(f"📊 **Calculation**: ((First - Second) / Second) × 100 = (({val_col1} - {val_col2}) / {val_col2}) × 100")

        # Highlighting mode selection
        highlight_mode = st.radio(
            "Highlighting mode:",
            ["Entire row", "Percentage difference column only"],
            key="dc_abs_highlight_mode"
        )
        highlight_mode_value = "entire_row" if highlight_mode == "Entire row" else "percentage_column"

        if st.button("🔍 Process Analysis", key="dc_abs_process"):
            with st.spinner("Processing..."):
                try:
                    # Determine dividend column based on user selection
                    dividend_col_abs = val_col1 if dividend_choice_abs == "First column" else val_col2
                    
                    result = checker.process_absolute_percentage_mode(
                        val_col1, val_col2, threshold, highlight_mode_value, dividend_col_abs)
                    st.success("✅ Analysis completed!")

                    # Dynamic filename based on mode
                    filename = f"absolute_percentage_analysis_{'merged' if not single_file_mode else 'single'}.xlsx"
                    create_download_link(
                        result,
                        filename,
                        "📥 Download Analysis Results"
                    )
                except Exception as e:
                    st.error(f"❌ Error during processing: {str(e)}")

    else:  # Range mode
        col1, col2, col3 = st.columns(3)
        with col1:
            anchor_col = st.selectbox(
                "Anchor column:", columns, key="dc_anchor")
        with col2:
            low_col = st.selectbox(
                "Low boundary column:", columns, key="dc_low")
        with col3:
            high_col = st.selectbox(
                "High boundary column:", columns, key="dc_high")

        # Highlighting mode selection for range mode
        highlight_mode_range = st.radio(
            "Highlighting mode:",
            ["Entire row", "Anchor column only"],
            key="dc_highlight_mode_range"
        )
        highlight_mode_range_value = "entire_row" if highlight_mode_range == "Entire row" else "anchor_column"

        if st.button("🔍 Process Analysis", key="dc_process_range"):
            with st.spinner("Processing..."):
                try:
                    result = checker.process_range_mode(
                        anchor_col, low_col, high_col, highlight_mode_range_value)
                    st.success("✅ Analysis completed!")

                    # Dynamic filename based on mode
                    filename = f"range_analysis_{'merged' if not single_file_mode else 'single'}.xlsx"
                    create_download_link(
                        result,
                        filename,
                        "📥 Download Analysis Results"
                    )
                except Exception as e:
                    st.error(f"❌ Error during processing: {str(e)}")


def discrepancy_analyzer_tab():
    """Discrepancy Analyzer workflow tab"""
    st.markdown("""
    <div class="workflow-card">
        <h3>📈 Discrepancy Analyzer</h3>
        <p>Generate comprehensive vehicle valuation discrepancy reports with detailed analysis 
        by brand, model year, and price range.</p>
    </div>
    """, unsafe_allow_html=True)

    # Mode selection
    st.subheader("📋 Analysis Mode")
    file_mode = st.radio(
        "Choose file mode:",
        ["Single file (all columns from one file)",
         "Two files (join data from separate file)"],
        key="da_file_mode"
    )

    single_file_mode = file_mode.startswith("Single file")

    # File uploads
    if single_file_mode:
        st.subheader("📄 Data File")
        primary_file = st.file_uploader(
            "Upload data file (Excel/CSV)",
            type=['xlsx', 'xls', 'csv'],
            key="da_single_file"
        )
        join_file = None
    else:
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("📄 Primary Data File")
            primary_file = st.file_uploader(
                "Upload primary data file (Excel/CSV)",
                type=['xlsx', 'xls', 'csv'],
                key="da_primary_file"
            )
        with col2:
            st.subheader("📄 Join Data File")
            join_file = st.file_uploader(
                "Upload join data file (Excel/CSV)",
                type=['xlsx', 'xls', 'csv'],
                key="da_join_file"
            )

    # Process files based on mode
    if single_file_mode and primary_file is not None:
        analyzer = DiscrepancyAnalyzer()

        try:
            # Load primary data
            primary_type = get_file_type(primary_file)
            primary_data = io.BytesIO(primary_file.read())
            primary_sheets = analyzer.load_primary_data(
                primary_data, primary_type)

            # Sheet selection
            if len(primary_sheets) > 1:
                primary_sheet = st.selectbox(
                    "Select data sheet:", primary_sheets, key="da_single_sheet")
            else:
                primary_sheet = primary_sheets[0]
                st.info(f"Using sheet: {primary_sheet}")

            # Load selected sheet
            primary_data.seek(0)
            primary_df = analyzer.select_primary_sheet(
                primary_sheet, primary_data, primary_type)

            # Clean DataFrame immediately after loading to prevent PyArrow issues
            primary_df = clean_dataframe_for_processing(primary_df)
            analyzer.primary_data = primary_df

            st.success(f"✅ Loaded {len(primary_df)} records")

            # Column selection for single file mode
            st.subheader("📋 Column Configuration")
            primary_columns = analyzer.get_primary_columns()

            col1, col2 = st.columns(2)
            with col1:
                vin_col = st.selectbox(
                    "VIN column:", primary_columns, key="da_single_vin")
                make_col = st.selectbox(
                    "Make column:", primary_columns, key="da_single_make")
                model_col = st.selectbox(
                    "Model column:", primary_columns, key="da_single_model")

            with col2:
                year_col = st.selectbox(
                    "ModelYear column:", primary_columns, key="da_single_year")

            # Analysis mode
            st.subheader("⚙️ Analysis Configuration")
            no_calc = st.checkbox(
                "Use precomputed percentage difference column", key="da_single_no_calc")

            if no_calc:
                percent_diff_col = st.selectbox(
                    "Percentage difference column:", primary_columns, key="da_single_percent_diff")
                price_col = st.selectbox(
                    "Price column (for price range analysis):", primary_columns, key="da_single_price_col")
                val_col1, val_col2 = None, None
            else:
                col1, col2 = st.columns(2)
                with col1:
                    val_col1 = st.selectbox(
                        "First value column:", primary_columns, key="da_single_val1")
                with col2:
                    val_col2 = st.selectbox(
                        "Second value column:", primary_columns, key="da_single_val2")
                percent_diff_col = None
                price_col = None

            threshold = st.number_input(
                "Threshold percentage:", min_value=0.0, max_value=100.0, value=15.0, key="da_single_threshold")

            if st.button("🔍 Generate Report", key="da_single_generate"):
                with st.spinner("Generating report..."):
                    try:
                        report = analyzer.generate_report(
                            vin_col=vin_col,
                            no_calc=no_calc,
                            threshold=threshold,
                            val_col1=val_col1,
                            val_col2=val_col2,
                            percent_diff_col=percent_diff_col,
                            vin_join_col=None,  # Not used in single file mode
                            make_col=make_col,
                            model_col=model_col,
                            year_col=year_col,
                            price_col=price_col,
                            single_file_mode=True
                        )

                        st.success("✅ Report generated successfully!")

                        create_download_link(
                            report,
                            f"discrepancy_report_{int(time.time())}.xlsx",
                            "📥 Download Discrepancy Report"
                        )

                    except Exception as e:
                        st.error(f"❌ Error generating report: {str(e)}")

        except Exception as e:
            st.error(f"❌ Error loading file: {str(e)}")
            return

    elif not single_file_mode and primary_file is not None and join_file is not None:
        analyzer = DiscrepancyAnalyzer()

        try:
            # Load primary data
            primary_type = get_file_type(primary_file)
            primary_data = io.BytesIO(primary_file.read())
            primary_sheets = analyzer.load_primary_data(
                primary_data, primary_type)

            # Load join data
            join_type = get_file_type(join_file)
            join_data = io.BytesIO(join_file.read())
            join_sheets = analyzer.load_join_data(join_data, join_type)

            # Sheet selection
            col1, col2 = st.columns(2)
            with col1:
                if len(primary_sheets) > 1:
                    primary_sheet = st.selectbox(
                        "Select primary data sheet:", primary_sheets, key="da_primary_sheet")
                else:
                    primary_sheet = primary_sheets[0]
                    st.info(f"Using sheet: {primary_sheet}")

            with col2:
                if len(join_sheets) > 1:
                    join_sheet = st.selectbox(
                        "Select join data sheet:", join_sheets, key="da_join_sheet")
                else:
                    join_sheet = join_sheets[0]
                    st.info(f"Using sheet: {join_sheet}")

            # Load selected sheets
            primary_data.seek(0)
            primary_df = analyzer.select_primary_sheet(
                primary_sheet, primary_data, primary_type)
            join_data.seek(0)
            join_df = analyzer.select_join_sheet(
                join_sheet, join_data, join_type)

            # Clean DataFrames immediately after loading to prevent PyArrow issues
            primary_df = clean_dataframe_for_processing(primary_df)
            join_df = clean_dataframe_for_processing(join_df)
            # Update analyzer's internal dataframes
            analyzer.primary_data = primary_df
            analyzer.join_data = join_df

            st.success(
                f"✅ Loaded {len(primary_df)} primary records and {len(join_df)} join records")

        except Exception as e:
            st.error(f"❌ Error loading files: {str(e)}")
            return

        # Column selection for two file mode
        st.subheader("📋 Column Configuration")

        primary_columns = analyzer.get_primary_columns()
        join_columns = analyzer.get_join_columns()

        # Primary data columns
        col1, col2 = st.columns(2)
        with col1:
            st.write("**Primary Data Columns:**")
            vin_col = st.selectbox(
                "VIN column:", primary_columns, key="da_vin")

        with col2:
            st.write("**Join Data Columns:**")
            vin_join_col = st.selectbox(
                "VIN column:", join_columns, key="da_vin_join")

        # Join data columns
        col1, col2, col3 = st.columns(3)
        with col1:
            make_col = st.selectbox(
                "Make column:", join_columns, key="da_make")
        with col2:
            model_col = st.selectbox(
                "Model column:", join_columns, key="da_model")
        with col3:
            year_col = st.selectbox(
                "ModelYear column:", join_columns, key="da_year")

        # Analysis mode
        st.subheader("⚙️ Analysis Configuration")

        no_calc = st.checkbox(
            "Use precomputed percentage difference column", key="da_no_calc")

        if no_calc:
            percent_diff_col = st.selectbox(
                "Percentage difference column:", primary_columns, key="da_percent_diff")
            price_col = st.selectbox(
                "Price column (for price range analysis):", primary_columns, key="da_price_col")
            val_col1, val_col2 = None, None
        else:
            col1, col2 = st.columns(2)
            with col1:
                val_col1 = st.selectbox(
                    "First value column:", primary_columns, key="da_val1")
            with col2:
                val_col2 = st.selectbox(
                    "Second value column:", primary_columns, key="da_val2")
            percent_diff_col = None
            price_col = None

        threshold = st.number_input(
            "Threshold percentage:", min_value=0.0, max_value=100.0, value=15.0, key="da_threshold")

        if st.button("🔍 Generate Report", key="da_generate"):
            with st.spinner("Generating report..."):
                try:
                    report = analyzer.generate_report(
                        vin_col=vin_col,
                        no_calc=no_calc,
                        threshold=threshold,
                        val_col1=val_col1,
                        val_col2=val_col2,
                        percent_diff_col=percent_diff_col,
                        vin_join_col=vin_join_col,
                        make_col=make_col,
                        model_col=model_col,
                        year_col=year_col,
                        price_col=price_col,
                        single_file_mode=False
                    )

                    st.success("✅ Report generated successfully!")

                    create_download_link(
                        report,
                        f"discrepancy_report_{int(time.time())}.xlsx",
                        "📥 Download Discrepancy Report"
                    )

                except Exception as e:
                    st.error(f"❌ Error generating report: {str(e)}")

    elif not single_file_mode:
        st.info("Please upload both primary and join data files to proceed.")
    else:
        st.info("Please upload a data file to proceed.")


def spec_mapper_tab():
    """Spec Mapper workflow tab"""
    st.markdown("""
    <div class="workflow-card">
        <h3>🎯 Specification Mapper</h3>
        <p>Advanced fuzzy matching for vehicle specifications with special handling for Mercedes-Benz and BMW. 
        Maps make, model, and trim data to a standardized format.</p>
    </div>
    """, unsafe_allow_html=True)

    # File uploads
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("📄 Input Data File")
        input_file = st.file_uploader(
            "Upload input specifications file",
            type=['xlsx', 'xls', 'csv'],
            key="sm_input_file"
        )

    with col2:
        st.subheader("📄 Reference Data File")
        reference_file = st.file_uploader(
            "Upload reference/master data file",
            type=['xlsx', 'xls', 'csv'],
            key="sm_reference_file"
        )

    if input_file is not None and reference_file is not None:
        mapper = SpecMapper()

        try:
            # Load input data
            input_type = get_file_type(input_file)
            input_data = io.BytesIO(input_file.read())
            input_sheets = mapper.load_input_data(input_data, input_type)

            # Load reference data
            reference_type = get_file_type(reference_file)
            reference_data = io.BytesIO(reference_file.read())
            reference_sheets = mapper.load_reference_data(
                reference_data, reference_type)

            # Sheet selection
            col1, col2 = st.columns(2)
            with col1:
                if len(input_sheets) > 1:
                    input_sheet = st.selectbox(
                        "Select input data sheet:", input_sheets, key="sm_input_sheet")
                else:
                    input_sheet = input_sheets[0]
                    st.info(f"Using sheet: {input_sheet}")

            with col2:
                if len(reference_sheets) > 1:
                    reference_sheet = st.selectbox(
                        "Select reference data sheet:", reference_sheets, key="sm_reference_sheet")
                else:
                    reference_sheet = reference_sheets[0]
                    st.info(f"Using sheet: {reference_sheet}")

            # Load selected sheets
            input_data.seek(0)
            input_df = mapper.select_input_sheet(
                input_sheet, input_data, input_type)
            reference_data.seek(0)
            reference_df = mapper.select_reference_sheet(
                reference_sheet, reference_data, reference_type)

            # Clean DataFrames immediately after loading to prevent PyArrow issues
            input_df = clean_dataframe_for_processing(input_df)
            reference_df = clean_dataframe_for_processing(reference_df)
            # Update mapper's internal dataframes
            mapper.input_data = input_df
            mapper.reference_data = reference_df

            st.success(
                f"✅ Loaded {len(input_df)} input records and {len(reference_df)} reference records")

        except Exception as e:
            st.error(f"❌ Error loading files: {str(e)}")
            return

        # Column configuration
        st.subheader("📋 Column Configuration")

        input_columns = mapper.get_input_columns()
        reference_columns = mapper.get_reference_columns()

        # Input file columns
        col1, col2 = st.columns(2)
        with col1:
            st.write("**Input File Columns:**")
            input_make = st.selectbox(
                "Make column:", input_columns, key="sm_input_make")
            input_model = st.selectbox(
                "Model column:", input_columns, key="sm_input_model")
            input_trim = st.selectbox("Trim column (optional):", [
                                      ""] + input_columns, key="sm_input_trim")

        with col2:
            st.write("**Reference File Columns:**")
            ref_make = st.selectbox(
                "Make column:", reference_columns, key="sm_ref_make")
            ref_model = st.selectbox(
                "Model column:", reference_columns, key="sm_ref_model")
            ref_trim = st.selectbox("Trim column (optional):", [
                                    ""] + reference_columns, key="sm_ref_trim")

        # Mapping options
        st.subheader("⚙️ Mapping Configuration")

        col1, col2 = st.columns(2)
        with col1:
            skip_trim = st.checkbox("Skip trim mapping", key="sm_skip_trim")
            use_original_on_no_match = st.checkbox(
                "Use original values when no mapping found",
                key="sm_use_original",
                help="When enabled, unmapped values will use the original input value instead of being left blank in consolidated data"
            )
            skip_special_brands = st.checkbox(
                "Disable Mercedes/BMW special processing",
                key="sm_skip_special_brands",
                help="When enabled, Mercedes and BMW vehicles will be processed using standard fuzzy matching instead of special extraction patterns"
            )

            # Gemini AI Verification
            use_gemini_verification = st.checkbox(
                "Enable Gemini AI verification for makes",
                key="sm_use_gemini",
                help="Use Gemini AI to verify that fuzzy-matched makes actually refer to the same manufacturer. Failed verifications move to unmatched."
            )

            if use_gemini_verification:
                if not os.getenv('GEMINI_API_KEY'):
                    st.warning("⚠️ GEMINI_API_KEY not found in .env file. Verification will be skipped.")
                else:
                    st.info("✓ Gemini verification enabled. This will verify all matched makes.")

        with col2:
            # Advanced thresholds
            with st.expander("🔧 Advanced Settings"):
                make_threshold = st.slider(
                    "Make threshold:", 0, 100, 80, key="sm_make_threshold")
                model_threshold = st.slider(
                    "Model threshold:", 0, 100, 80, key="sm_model_threshold")
                trim_threshold = st.slider(
                    "Trim threshold:", 0, 100, 80, key="sm_trim_threshold")

                st.write("**Fuzzy Matching Methods:**")
                st.info("📌 Configure different matching algorithms for each dimension")

                # Create columns for dimension-specific methods
                method_col1, method_col2, method_col3 = st.columns(3)

                fuzzy_methods = [
                    "default",
                    "ratio",
                    "partial_ratio",
                    "token_sort_ratio",
                    "token_set_ratio",
                    "partial_token_sort_ratio",
                    "partial_token_set_ratio"
                ]

                with method_col1:
                    fuzzy_method_make = st.selectbox(
                        "Make method:",
                        options=fuzzy_methods,
                        index=0,
                        key="sm_fuzzy_method_make",
                        help="Algorithm for matching makes (uses simple ratio by default)"
                    )

                with method_col2:
                    fuzzy_method_model = st.selectbox(
                        "Model method:",
                        options=fuzzy_methods,
                        index=0,
                        key="sm_fuzzy_method_model",
                        help="Algorithm for matching models (uses token-based matching by default)"
                    )

                with method_col3:
                    fuzzy_method_trim = st.selectbox(
                        "Trim method:",
                        options=fuzzy_methods,
                        index=0,
                        key="sm_fuzzy_method_trim",
                        help="Algorithm for matching trims (uses token-based matching by default)"
                    )

        # Handle optional trim columns
        if input_trim == "":
            input_trim = None
        if ref_trim == "":
            ref_trim = None

        if skip_trim:
            input_trim = None
            ref_trim = None

        if st.button("🎯 Start Mapping", key="sm_map"):
            with st.spinner("Performing specification mapping..."):
                try:
                    # Create column config
                    column_config = ColumnConfig(
                        input_make=input_make,
                        input_model=input_model,
                        input_trim=input_trim,
                        ref_make=ref_make,
                        ref_model=ref_model,
                        ref_trim=ref_trim
                    )

                    # Perform mapping
                    results = mapper.map_specifications(
                        column_config=column_config,
                        skip_trim=skip_trim,
                        make_threshold=make_threshold,
                        model_threshold=model_threshold,
                        trim_threshold=trim_threshold,
                        use_original_on_no_match=use_original_on_no_match,
                        method_make=fuzzy_method_make,
                        method_model=fuzzy_method_model,
                        method_trim=fuzzy_method_trim,
                        skip_special_brands=skip_special_brands,
                        use_gemini_verification=use_gemini_verification
                    )

                    # Display results summary
                    st.success("✅ Mapping completed!")

                    # Show statistics
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Makes Mapped", len(results['mapped_makes']))
                    with col2:
                        st.metric("Models Mapped", len(
                            results['mapped_models']))
                    with col3:
                        if 'mapped_trims' in results:
                            st.metric("Trims Mapped", len(
                                results['mapped_trims']))
                        else:
                            st.metric("Trims Mapped", "N/A")

                    # Save results
                    files = mapper.save_results(
                        results, input_df, column_config, use_original_on_no_match)

                    # Create download options
                    st.subheader("📥 Download Results")

                    # Individual file downloads
                    for filename, file_data in files.items():
                        create_download_link(
                            file_data, filename, f"📄 {filename}")

                    # Zip download
                    create_zip_download(files, "specification_mapping_results")

                    # Display sample results
                    st.subheader("📊 Sample Results")

                    if not results['mapped_makes'].empty:
                        st.write("**Mapped Makes (Sample):**")
                        safe_display_dataframe(
                            results['mapped_makes'], 5, "Mapped Makes Sample")

                    if not results['mapped_models'].empty:
                        st.write("**Mapped Models (Sample):**")
                        safe_display_dataframe(
                            results['mapped_models'], 5, "Mapped Models Sample")

                except Exception as e:
                    st.error(f"❌ Error during mapping: {str(e)}")


def vehicle_verifier_tab():
    """Vehicle Data Verifier workflow tab"""
    st.markdown("""
    <div class="workflow-card">
        <h3>🔍 Vehicle Data Verifier</h3>
        <p>Verify vehicle data between reference and logs with optional Arabic translation support. 
        Provides detailed match analysis with conditional formatting.</p>
    </div>
    """, unsafe_allow_html=True)

    # File uploads
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("📄 Reference Data File")
        reference_file = st.file_uploader(
            "Upload reference data file",
            type=['xlsx', 'xls', 'csv'],
            key="vv_reference_file"
        )

    with col2:
        st.subheader("📄 Logs Data File")
        logs_file = st.file_uploader(
            "Upload logs data file",
            type=['xlsx', 'xls', 'csv'],
            key="vv_logs_file"
        )

    if reference_file is not None and logs_file is not None:
        # Create a unique key for current files to detect file changes
        current_files_key = f"{reference_file.name}_{logs_file.name}_{reference_file.size}_{logs_file.size}"
        
        # Initialize or reset verifier when files change
        if 'verifier' not in st.session_state or st.session_state.get('vv_files_key') != current_files_key:
            st.session_state.verifier = VehicleDataVerifier()
            st.session_state.vv_files_key = current_files_key
            st.session_state.vv_data_prepared = False
            
        verifier = st.session_state.verifier

        try:
            # Load reference data
            reference_type = get_file_type(reference_file)
            reference_data = io.BytesIO(reference_file.read())
            reference_sheets = verifier.load_reference_data(
                reference_data, reference_type)

            # Load logs data
            logs_type = get_file_type(logs_file)
            logs_data = io.BytesIO(logs_file.read())
            logs_sheets = verifier.load_logs_data(logs_data, logs_type)

            # Sheet selection
            col1, col2 = st.columns(2)
            with col1:
                if len(reference_sheets) > 1:
                    reference_sheet = st.selectbox(
                        "Select reference data sheet:", reference_sheets, key="vv_reference_sheet")
                else:
                    reference_sheet = reference_sheets[0]
                    st.info(f"Using sheet: {reference_sheet}")

            with col2:
                if len(logs_sheets) > 1:
                    logs_sheet = st.selectbox(
                        "Select logs data sheet:", logs_sheets, key="vv_logs_sheet")
                else:
                    logs_sheet = logs_sheets[0]
                    st.info(f"Using sheet: {logs_sheet}")

            # Load selected sheets
            reference_data.seek(0)
            reference_df = verifier.select_reference_sheet(
                reference_sheet, reference_data, reference_type)
            logs_data.seek(0)
            logs_df = verifier.select_logs_sheet(
                logs_sheet, logs_data, logs_type)

            # Clean DataFrames immediately after loading to prevent PyArrow issues
            reference_df = clean_dataframe_for_processing(reference_df)
            logs_df = clean_dataframe_for_processing(logs_df)
            # Update verifier's internal dataframes
            verifier.reference_data = reference_df
            verifier.logs_data = logs_df

            st.success(
                f"✅ Loaded {len(reference_df)} reference records and {len(logs_df)} logs records")

        except Exception as e:
            st.error(f"❌ Error loading files: {str(e)}")
            return

        # Column configuration
        st.subheader("📋 Column Configuration")

        reference_columns = verifier.get_reference_columns()
        logs_columns = verifier.get_logs_columns()

        # Reference data columns
        col1, col2 = st.columns(2)
        with col1:
            st.write("**Reference Data Columns:**")
            chassis_col = st.selectbox(
                "Chassis/VIN column:", reference_columns, key="vv_chassis")
            make_ext_col = st.selectbox(
                "Make column:", reference_columns, key="vv_make_ext")
            model_ext_col = st.selectbox(
                "Model column:", reference_columns, key="vv_model_ext")
            year_ext_col = st.selectbox(
                "ModelYear column:", reference_columns, key="vv_year_ext")

        with col2:
            st.write("**Logs Data Columns:**")
            vin_col = st.selectbox("VIN column:", logs_columns, key="vv_vin")
            make_col = st.selectbox(
                "Make column:", logs_columns, key="vv_make")
            model_col = st.selectbox(
                "Model column:", logs_columns, key="vv_model")
            year_col = st.selectbox(
                "ModelYear column:", logs_columns, key="vv_year")
            spec_status_col = st.selectbox(
                "Specification Status column:", logs_columns, key="vv_spec_status")

        # Translation section (always available after data is loaded)
        if True:  # Translation is now always available
            st.subheader("🌐 Translation (Optional)")

            if TRANSLATION_AVAILABLE:
                enable_translation = st.checkbox(
                    "Enable Arabic translation", key="vv_enable_translation")

                if enable_translation:
                    api_key = st.text_input(
                        "OpenAI API Key:", type="password", key="vv_api_key")

                    if api_key and st.button("🔄 Translate Arabic Text", key="vv_translate"):
                        progress_bar = st.progress(0)
                        status_text = st.empty()

                        def update_progress(current, total):
                            progress = current / total
                            progress_bar.progress(progress)
                            status_text.text(
                                f"Translating... {current}/{total}")

                        with st.spinner("Translating Arabic text..."):
                            try:
                                # First prepare data for translation
                                verifier.prepare_reference_data(
                                    chassis_col, make_ext_col, model_ext_col, year_ext_col)
                                
                                result = asyncio.run(
                                    verifier.perform_translation(api_key, update_progress))

                                if "error" in result:
                                    st.error(
                                        f"❌ Translation error: {result['error']}")
                                else:
                                    st.success(f"✅ {result['status']}")
                                    if 'brand_translations' in result:
                                        st.info(
                                            f"Translated {result['brand_translations']} brands and {result['model_translations']} models")

                            except Exception as e:
                                st.error(f"❌ Translation failed: {str(e)}")

                        progress_bar.empty()
                        status_text.empty()
            else:
                st.info(
                    "💡 Translation feature requires OpenAI library. Install it to enable translation.")

            # Verification section
            st.subheader("✅ Verification")
            
            # Mask columns toggle
            include_mask_in_main = st.checkbox(
                "Include match columns in main sheet", 
                value=True, 
                key="vv_include_mask",
                help="Include Make Match, Model Match, and Year Match columns directly in the analysis sheet"
            )

            if st.button("🔍 Perform Verification", key="vv_verify"):
                with st.spinner("Performing verification..."):
                    try:
                        results = verifier.perform_verification(
                            chassis_col, make_ext_col, model_ext_col, year_ext_col,
                            vin_col, make_col, model_col, year_col, spec_status_col
                        )

                        st.success("✅ Verification completed!")

                        # Display summary
                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            st.metric("Total Records",
                                      results['total_records'])
                        with col2:
                            st.metric(
                                "Make Matches", f"{results['make_matches']} ({results['make_match_percentage']:.1f}%)")
                        with col3:
                            st.metric(
                                "Model Matches", f"{results['model_matches']} ({results['model_match_percentage']:.1f}%)")
                        with col4:
                            st.metric(
                                "Year Matches", f"{results['year_matches']} ({results['year_match_percentage']:.1f}%)")

                        # Show sample mismatches
                        if results['mismatches_count'] > 0:
                            st.subheader("🔍 Sample Mismatches")
                            sample_mismatches = verifier.get_sample_mismatches(
                                10)
                            safe_display_dataframe(
                                sample_mismatches, 10, "Sample Mismatches")

                        # Generate report
                        st.subheader("📥 Download Results")

                        report = verifier.save_results(include_mask_in_main=include_mask_in_main)
                        create_download_link(
                            report,
                            f"verification_report_{int(time.time())}.xlsx",
                            "📥 Download Verification Report"
                        )

                    except Exception as e:
                        st.error(f"❌ Error during verification: {str(e)}")


def translation_service_tab():
    """Translation Service workflow tab"""
    st.markdown("""
    <div class="workflow-card">
        <h3>🌐 Translation Service</h3>
        <p>Standalone Arabic to English translation service for vehicle specifications. 
        Upload files with Arabic text and get translated results with progress tracking.</p>
    </div>
    """, unsafe_allow_html=True)

    # Check if translation service is available
    if not TRANSLATION_SERVICE_AVAILABLE:
        st.error("❌ Translation service not available. Please install the OpenAI library.")
        st.info("💡 Install with: `pip install openai`")
        return

    # File upload
    uploaded_file = st.file_uploader(
        "Upload Excel or CSV file containing Arabic text",
        type=['xlsx', 'xls', 'csv'],
        key="translation_service_file"
    )

    if uploaded_file is not None:
        file_type = get_file_type(uploaded_file)

        if file_type == 'unknown':
            st.error("❌ Unsupported file type. Please upload Excel (.xlsx, .xls) or CSV files.")
            return

        try:
            # Load data
            file_data = io.BytesIO(uploaded_file.read())
            
            if file_type in ['xlsx', 'xls']:
                xls = pd.ExcelFile(file_data)
                sheets = xls.sheet_names
                
                # Sheet selection
                if len(sheets) > 1:
                    selected_sheet = st.selectbox("Select sheet:", sheets, key="ts_sheet")
                else:
                    selected_sheet = sheets[0]
                    st.info(f"Using sheet: {selected_sheet}")
                
                # Load selected sheet
                file_data.seek(0)
                df = pd.read_excel(file_data, sheet_name=selected_sheet)
            else:
                df = pd.read_csv(file_data, encoding='utf-8')
                selected_sheet = "Data"

            # Clean DataFrame
            df = clean_dataframe_for_processing(df)

            st.success(f"✅ Loaded {len(df)} records from '{selected_sheet}'")
            st.write("**Data Preview:**")
            safe_display_dataframe(df, 5, "Data Preview")

        except Exception as e:
            st.error(f"❌ Error loading file: {str(e)}")
            return

        # Column selection for translation
        st.subheader("📋 Translation Configuration")
        
        available_columns = list(df.columns)
        
        # Multi-select for columns to translate
        columns_to_translate = st.multiselect(
            "Select columns to translate from Arabic to English:",
            available_columns,
            key="ts_columns"
        )

        if not columns_to_translate:
            st.warning("⚠️ Please select at least one column to translate.")
            return

        # Preview Arabic content
        translation_service = ArabicTranslationService()
        
        # Show preview of Arabic content in selected columns
        st.subheader("🔍 Arabic Content Preview")
        arabic_found = False
        for col in columns_to_translate:
            if col in df.columns:
                # Check for Arabic content
                col_values = df[col].astype(str).tolist()
                arabic_indexes = translation_service.get_arabic_indexes(col_values)
                
                if arabic_indexes:
                    arabic_found = True
                    arabic_samples = [col_values[i] for i in arabic_indexes[:3]]  # Show first 3 samples
                    st.write(f"**{col}:** {len(arabic_indexes)} entries with Arabic text")
                    st.write("Sample Arabic entries:", arabic_samples)

        if not arabic_found:
            st.warning("⚠️ No Arabic text found in selected columns.")
            return

        # Translation settings
        st.subheader("⚙️ Translation Settings")
        
        col1, col2 = st.columns(2)
        with col1:
            max_concurrent = st.slider(
                "Max concurrent requests:", 
                min_value=1, max_value=20, value=10, 
                key="ts_concurrent"
            )
        with col2:
            use_cache = st.checkbox(
                "Enable translation caching", 
                value=True, 
                key="ts_cache"
            )

        # API Key input
        api_key = st.text_input(
            "OpenAI API Key:", 
            type="password", 
            key="ts_api_key",
            help="Enter your OpenAI API key for translation"
        )

        # Translation button
        if st.button("🚀 Start Translation", key="ts_translate"):
            if not api_key:
                st.error("❌ Please provide your OpenAI API key.")
                return

            with st.spinner("Translating Arabic text..."):
                try:
                    # Initialize translation service with custom settings
                    translation_service = ArabicTranslationService(
                        max_concurrent_requests=max_concurrent
                    )

                    # Progress tracking
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    def update_progress(current, total):
                        progress = current / total if total > 0 else 0
                        progress_bar.progress(progress)
                        status_text.text(f"Translating... {current}/{total}")

                    # Perform translation
                    translated_df = asyncio.run(
                        translation_service.translate_dataframe_columns(
                            df.copy(),
                            columns_to_translate,
                            api_key,
                            progress_callback=update_progress,
                            use_cache=use_cache
                        )
                    )

                    progress_bar.empty()
                    status_text.empty()

                    st.success("✅ Translation completed!")

                    # Show translation statistics
                    cache_stats = translation_service.get_cache_stats()
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Columns Translated", len(columns_to_translate))
                    with col2:
                        st.metric("Records Processed", len(translated_df))
                    with col3:
                        st.metric("Cached Translations", cache_stats['cached_translations'])

                    # Show sample translated results
                    st.subheader("📊 Translation Results")
                    
                    # Display before/after comparison for selected columns
                    for col in columns_to_translate[:2]:  # Show first 2 columns
                        if col in df.columns:
                            st.write(f"**Column: {col}**")
                            
                            # Create comparison DataFrame
                            comparison_data = {
                                'Original': df[col].head(5).tolist(),
                                'Translated': translated_df[col].head(5).tolist()
                            }
                            comparison_df = pd.DataFrame(comparison_data)
                            safe_display_dataframe(comparison_df, 5, f"{col} Comparison")

                    # Download options
                    st.subheader("📥 Download Results")
                    
                    # Save as Excel
                    output_excel = io.BytesIO()
                    with pd.ExcelWriter(output_excel, engine='openpyxl') as writer:
                        translated_df.to_excel(writer, sheet_name='Translated_Data', index=False)
                        
                        # Also save original for comparison
                        df.to_excel(writer, sheet_name='Original_Data', index=False)
                        
                        # Create summary sheet
                        summary_data = {
                            'Metric': ['Total Records', 'Columns Translated', 'Translation Service', 'Timestamp'],
                            'Value': [
                                len(translated_df),
                                len(columns_to_translate),
                                'ArabicTranslationService',
                                pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
                            ]
                        }
                        summary_df = pd.DataFrame(summary_data)
                        summary_df.to_excel(writer, sheet_name='Summary', index=False)

                    output_excel.seek(0)

                    create_download_link(
                        output_excel,
                        f"translated_data_{int(time.time())}.xlsx",
                        "📥 Download Translated Data (Excel)"
                    )

                    # Save as CSV (just the translated data)
                    output_csv = io.BytesIO()
                    translated_df.to_csv(output_csv, index=False, encoding='utf-8')
                    output_csv.seek(0)

                    create_download_link(
                        output_csv,
                        f"translated_data_{int(time.time())}.csv",
                        "📄 Download Translated Data (CSV)"
                    )

                except Exception as e:
                    st.error(f"❌ Translation failed: {str(e)}")
                    progress_bar.empty()
                    status_text.empty()


def combined_verifier_checker_tab():
    """Combined Vehicle Data Verifier and Discrepancy Checker workflow tab"""
    st.markdown("""
    <div class="workflow-card">
        <h3>🔗 Combined Verifier & Checker</h3>
        <p>Comprehensive analysis combining vehicle data verification with discrepancy checking. 
        Verify vehicle data between reference and logs, then analyze value discrepancies with conditional formatting.</p>
    </div>
    """, unsafe_allow_html=True)

    # File uploads
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("📄 Reference Data File")
        reference_file = st.file_uploader(
            "Upload reference data file",
            type=['xlsx', 'xls', 'csv'],
            key="cvc_reference_file"
        )

    with col2:
        st.subheader("📄 Logs Data File")
        logs_file = st.file_uploader(
            "Upload logs data file",
            type=['xlsx', 'xls', 'csv'],
            key="cvc_logs_file"
        )

    if reference_file is not None and logs_file is not None:
        # Create a unique key for current files to detect file changes
        current_files_key = f"{reference_file.name}_{logs_file.name}_{reference_file.size}_{logs_file.size}"
        
        # Initialize or reset combined analyzer when files change
        if 'cvc_analyzer' not in st.session_state or st.session_state.get('cvc_files_key') != current_files_key:
            st.session_state.cvc_analyzer = CombinedVerifierChecker()
            st.session_state.cvc_files_key = current_files_key
            
        analyzer = st.session_state.cvc_analyzer

        try:
            # Load reference data
            reference_type = get_file_type(reference_file)
            reference_data = io.BytesIO(reference_file.read())
            reference_sheets = analyzer.load_reference_data(reference_data, reference_type)

            # Load logs data
            logs_type = get_file_type(logs_file)
            logs_data = io.BytesIO(logs_file.read())
            logs_sheets = analyzer.load_logs_data(logs_data, logs_type)

            # Sheet selection
            col1, col2 = st.columns(2)
            with col1:
                if len(reference_sheets) > 1:
                    reference_sheet = st.selectbox(
                        "Select reference data sheet:", reference_sheets, key="cvc_reference_sheet")
                else:
                    reference_sheet = reference_sheets[0]
                    st.info(f"Using sheet: {reference_sheet}")

            with col2:
                if len(logs_sheets) > 1:
                    logs_sheet = st.selectbox(
                        "Select logs data sheet:", logs_sheets, key="cvc_logs_sheet")
                else:
                    logs_sheet = logs_sheets[0]
                    st.info(f"Using sheet: {logs_sheet}")

            # Load selected sheets
            reference_data.seek(0)
            reference_df = analyzer.select_reference_sheet(reference_sheet, reference_data, reference_type)
            logs_data.seek(0)
            logs_df = analyzer.select_logs_sheet(logs_sheet, logs_data, logs_type)

            # Clean DataFrames immediately after loading
            reference_df = clean_dataframe_for_processing(reference_df)
            logs_df = clean_dataframe_for_processing(logs_df)

            st.success(f"✅ Loaded {len(reference_df)} reference records and {len(logs_df)} logs records")

        except Exception as e:
            st.error(f"❌ Error loading files: {str(e)}")
            return

        # Column configuration
        st.subheader("📋 Column Configuration")

        reference_columns = analyzer.get_reference_columns()
        logs_columns = analyzer.get_logs_columns()

        # Vehicle verification columns
        st.write("**🔍 Vehicle Verification Columns:**")
        col1, col2 = st.columns(2)
        with col1:
            st.write("*Reference Data Columns:*")
            chassis_col = st.selectbox("Chassis/VIN column:", reference_columns, key="cvc_chassis")
            make_ext_col = st.selectbox("Make column:", reference_columns, key="cvc_make_ext")
            model_ext_col = st.selectbox("Model column:", reference_columns, key="cvc_model_ext")
            year_ext_col = st.selectbox("ModelYear column:", reference_columns, key="cvc_year_ext")

        with col2:
            st.write("*Logs Data Columns:*")
            vin_col = st.selectbox("VIN column:", logs_columns, key="cvc_vin")
            make_col = st.selectbox("Make column:", logs_columns, key="cvc_make")
            model_col = st.selectbox("Model column:", logs_columns, key="cvc_model")
            year_col = st.selectbox("ModelYear column:", logs_columns, key="cvc_year")
            spec_status_col = st.selectbox("Specification Status column:", logs_columns, key="cvc_spec_status")

        # Translation section (optional)
        st.subheader("🌐 Translation (Optional)")
        if TRANSLATION_AVAILABLE:
            enable_translation = st.checkbox("Enable Arabic translation", key="cvc_enable_translation")

            if enable_translation:
                api_key = st.text_input("OpenAI API Key:", type="password", key="cvc_api_key")

                if api_key and st.button("🔄 Translate Arabic Text", key="cvc_translate"):
                    progress_bar = st.progress(0)
                    status_text = st.empty()

                    def update_progress(current, total):
                        progress = current / total
                        progress_bar.progress(progress)
                        status_text.text(f"Translating... {current}/{total}")

                    with st.spinner("Translating Arabic text..."):
                        try:
                            result = asyncio.run(analyzer.perform_translation(api_key, update_progress))

                            if "error" in result:
                                st.error(f"❌ Translation error: {result['error']}")
                            else:
                                st.success(f"✅ {result['status']}")
                                if 'brand_translations' in result:
                                    st.info(f"Translated {result['brand_translations']} brands and {result['model_translations']} models")

                        except Exception as e:
                            st.error(f"❌ Translation failed: {str(e)}")

                    progress_bar.empty()
                    status_text.empty()
        else:
            st.info("💡 Translation feature requires OpenAI library. Install it to enable translation.")

        # Discrepancy Analysis Configuration
        st.subheader("📊 Discrepancy Analysis Configuration")
        
        # Get all available columns (will include merged columns after verification)
        all_columns = list(set(reference_columns + logs_columns))
        
        # Add info about merged data columns
        st.info("💡 After verification, additional columns will be available in the merged dataset. If selected columns don't exist after merge, discrepancy analysis will be skipped.")
        
        # Analysis mode selection
        analysis_mode = st.radio(
            "Select discrepancy analysis mode:",
            [
                "Compare two columns within % threshold",
                "Absolute % difference with color coding", 
                "Check value between low & high columns"
            ],
            key="cvc_analysis_mode"
        )

        # Configuration based on analysis mode
        val_col1, val_col2, threshold, dividend_col = None, None, 15.0, None
        anchor_col, low_col, high_col = None, None, None
        highlight_mode = "entire_row"

        if analysis_mode == "Compare two columns within % threshold":
            col1, col2 = st.columns(2)
            with col1:
                val_col1 = st.selectbox("First value column:", all_columns, key="cvc_val1")
            with col2:
                val_col2 = st.selectbox("Second value column:", all_columns, key="cvc_val2")

            threshold = st.number_input("Threshold percentage (e.g., 15 for 15%):",
                                       min_value=0.0, max_value=100.0, value=15.0, key="cvc_threshold")

            dividend_choice = st.radio(
                "Which column to use as dividend (denominator):",
                ["First column", "Second column"],
                index=1,
                key="cvc_dividend_choice"
            )
            dividend_col = val_col1 if dividend_choice == "First column" else val_col2

            highlight_mode = st.radio(
                "Highlighting mode:",
                ["Entire row", "Percentage difference column only"],
                key="cvc_highlight_mode"
            )
            highlight_mode = "entire_row" if highlight_mode == "Entire row" else "percentage_column"

        elif analysis_mode == "Absolute % difference with color coding":
            col1, col2 = st.columns(2)
            with col1:
                val_col1 = st.selectbox("First value column:", all_columns, key="cvc_abs_val1")
            with col2:
                val_col2 = st.selectbox("Second value column:", all_columns, key="cvc_abs_val2")

            threshold = st.number_input("Threshold percentage (e.g., 15 for 15%):",
                                       min_value=0.0, max_value=100.0, value=15.0, key="cvc_abs_threshold")

            dividend_choice = st.radio(
                "Which column to use as dividend (denominator):",
                ["First column", "Second column"],
                index=1,
                key="cvc_abs_dividend_choice"
            )
            dividend_col = val_col1 if dividend_choice == "First column" else val_col2

            highlight_mode = st.radio(
                "Highlighting mode:",
                ["Entire row", "Percentage difference column only"],
                key="cvc_abs_highlight_mode"
            )
            highlight_mode = "entire_row" if highlight_mode == "Entire row" else "percentage_column"

        else:  # Range mode
            col1, col2, col3 = st.columns(3)
            with col1:
                anchor_col = st.selectbox("Anchor column:", all_columns, key="cvc_anchor")
            with col2:
                low_col = st.selectbox("Low boundary column:", all_columns, key="cvc_low")
            with col3:
                high_col = st.selectbox("High boundary column:", all_columns, key="cvc_high")

            highlight_mode = st.radio(
                "Highlighting mode:",
                ["Entire row", "Anchor column only"],
                key="cvc_highlight_mode_range"
            )
            highlight_mode = "entire_row" if highlight_mode == "Entire row" else "anchor_column"

        # Mask columns toggle
        include_mask_in_main = st.checkbox(
            "Include verification match columns in main sheet", 
            value=True, 
            key="cvc_include_mask",
            help="Include Make Match, Model Match, and Year Match columns directly in the analysis sheet"
        )

        # Analysis execution
        st.subheader("🚀 Combined Analysis")
        
        if st.button("🔍 Perform Combined Analysis", key="cvc_analyze"):
            with st.spinner("Performing combined verification and discrepancy analysis..."):
                try:
                    results = analyzer.perform_combined_analysis(
                        # Verification parameters
                        chassis_col, make_ext_col, model_ext_col, year_ext_col,
                        vin_col, make_col, model_col, year_col, spec_status_col,
                        # Discrepancy parameters
                        analysis_mode, val_col1, val_col2, threshold, dividend_col,
                        highlight_mode, anchor_col, low_col, high_col
                    )

                    st.success("✅ Combined analysis completed!")

                    # Display verification summary
                    st.subheader("🔍 Verification Results")
                    verification_results = results['verification_results']
                    
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Total Records", verification_results['total_records'])
                    with col2:
                        st.metric("Make Matches", 
                                f"{verification_results['make_matches']} ({verification_results['make_match_percentage']:.1f}%)")
                    with col3:
                        st.metric("Model Matches", 
                                f"{verification_results['model_matches']} ({verification_results['model_match_percentage']:.1f}%)")
                    with col4:
                        st.metric("Year Matches", 
                                f"{verification_results['year_matches']} ({verification_results['year_match_percentage']:.1f}%)")

                    # Show sample mismatches
                    if verification_results['mismatches_count'] > 0:
                        st.subheader("🔍 Sample Verification Mismatches")
                        sample_mismatches = analyzer.get_sample_mismatches(5)
                        safe_display_dataframe(sample_mismatches, 5, "Sample Verification Mismatches")

                    # Display discrepancy analysis info
                    st.subheader("📊 Discrepancy Analysis Results")
                    st.success(f"✅ Combined analysis completed using: {analysis_mode}")
                    st.info("💡 Discrepancy analysis will be applied with Excel formulas and conditional formatting when you download the results.")

                    # Generate report
                    st.subheader("📥 Download Results")

                    try:
                        report = analyzer.save_combined_results(include_mask_in_main=include_mask_in_main)
                        create_download_link(
                            report,
                            f"combined_analysis_report_{int(time.time())}.xlsx",
                            "📥 Download Combined Analysis Report"
                        )

                        st.success("✅ Excel file generated with dynamic formulas and conditional formatting!")

                        st.info("""
                        **📋 Output Features:**
                        - **Dynamic Excel Formulas**: All calculations update automatically when you edit values
                        - **Verification Match Columns**: Make Match, Model Match, Year Match (if enabled)
                        - **Discrepancy Analysis**: Percentage difference or range check formulas
                        - **Conditional Formatting**: Automatic color-coding based on thresholds
                        - **Summary Sheet**: Verification statistics

                        **🔧 Editable & Recalculates Automatically!**
                        """)
                    except Exception as save_error:
                        st.error(f"❌ Error creating download file: {str(save_error)}")
                        import traceback
                        st.code(traceback.format_exc())
                        st.info("💡 Check the error details above for troubleshooting.")

                except Exception as e:
                    st.error(f"❌ Error during analysis: {str(e)}")

    else:
        st.info("Please upload both reference and logs data files to proceed.")


def old_new_validator_tab():
    """Old/New Data Validator workflow tab"""
    st.markdown("""
    <div class="workflow-card">
        <h3>🔄 Old/New Data Validator</h3>
        <p>Multi-dimensional comparison between old and new vehicle data files across 11 dimensions. 
        Compare ModelYear, Make, Model, Trim, BodyType, EngineSize, Transmission, Region, Doors, Seats, and Cylinders with VIN-based matching.</p>
    </div>
    """, unsafe_allow_html=True)

    # File uploads
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("📄 Old Data File")
        old_file = st.file_uploader(
            "Upload old vehicle data file",
            type=['xlsx', 'xls', 'csv'],
            key="onv_old_file"
        )

    with col2:
        st.subheader("📄 New Data File")
        new_file = st.file_uploader(
            "Upload new vehicle data file",
            type=['xlsx', 'xls', 'csv'],
            key="onv_new_file"
        )

    if old_file is not None and new_file is not None:
        # Create a unique key for current files to detect file changes
        current_files_key = f"{old_file.name}_{new_file.name}_{old_file.size}_{new_file.size}"
        
        # Initialize or reset validator when files change
        if 'onv_validator' not in st.session_state or st.session_state.get('onv_files_key') != current_files_key:
            st.session_state.onv_validator = OldNewDataValidator()
            st.session_state.onv_files_key = current_files_key
            
        validator = st.session_state.onv_validator

        try:
            # Load old data
            old_type = get_file_type(old_file)
            old_data = io.BytesIO(old_file.read())
            old_sheets = validator.load_old_data(old_data, old_type)

            # Load new data
            new_type = get_file_type(new_file)
            new_data = io.BytesIO(new_file.read())
            new_sheets = validator.load_new_data(new_data, new_type)

            # Sheet selection
            col1, col2 = st.columns(2)
            with col1:
                if len(old_sheets) > 1:
                    old_sheet = st.selectbox("Select old data sheet:", old_sheets, key="onv_old_sheet")
                else:
                    old_sheet = old_sheets[0]
                    st.info(f"Using sheet: {old_sheet}")

            with col2:
                if len(new_sheets) > 1:
                    new_sheet = st.selectbox("Select new data sheet:", new_sheets, key="onv_new_sheet")
                else:
                    new_sheet = new_sheets[0]
                    st.info(f"Using sheet: {new_sheet}")

            # Load selected sheets
            old_data.seek(0)
            old_df = validator.select_old_sheet(old_sheet, old_data, old_type)
            new_data.seek(0)
            new_df = validator.select_new_sheet(new_sheet, new_data, new_type)

            # Clean DataFrames immediately after loading to prevent PyArrow issues
            old_df = clean_dataframe_for_processing(old_df)
            new_df = clean_dataframe_for_processing(new_df)
            # Update validator's internal dataframes
            validator.old_data = old_df
            validator.new_data = new_df

            st.success(f"✅ Loaded {len(old_df)} old records and {len(new_df)} new records")

        except Exception as e:
            st.error(f"❌ Error loading files: {str(e)}")
            return

        # Column configuration
        st.subheader("📋 Column Configuration")
        
        old_columns = validator.get_old_columns()
        new_columns = validator.get_new_columns()

        # Default column names toggle
        use_default_columns = st.checkbox(
            "🎯 Use Default Column Names (Standard eData Structure)", 
            key="onv_use_defaults",
            help="Enable this if both files use standard eData column names (VIN, Model Year, Make, Model, etc.)"
        )
        
        # Default column mappings based on AmanaKSA structure analysis
        default_mappings = {
            'VIN': 'VIN',
            'ModelYear': 'Model Year',
            'Make': 'Make',
            'Model': 'Model',
            'Trim': 'Trim',
            'BodyType': 'Body Type',
            'EngineSize': 'Engine Size',
            'Transmission': 'Transmission',
            'Region': 'Region',
            'Doors': 'Doors',
            'Seats': 'Seats',
            'Cylinders': 'Cylinders'
        }

        if use_default_columns:
            # Validate that default columns exist in both files
            missing_old = [col for col in default_mappings.values() if col not in old_columns]
            missing_new = [col for col in default_mappings.values() if col not in new_columns]
            
            if missing_old or missing_new:
                st.error("❌ Default column names not found in files:")
                if missing_old:
                    st.error(f"Missing in old file: {', '.join(missing_old)}")
                if missing_new:
                    st.error(f"Missing in new file: {', '.join(missing_new)}")
                st.info("💡 Uncheck 'Use Default Column Names' to manually map columns.")
                return
            else:
                st.success("✅ All default columns found in both files!")
                old_column_mappings = default_mappings.copy()
                new_column_mappings = default_mappings.copy()
                
                # Show the default mappings being used
                with st.expander("📋 Default Column Mappings (Click to view)", expanded=False):
                    col1, col2 = st.columns(2)
                    with col1:
                        st.write("**Dimension → Column Name:**")
                        for dim, col_name in default_mappings.items():
                            st.write(f"• {dim} → `{col_name}`")
                    with col2:
                        st.info("""
                        **Using Standard eData Structure:**
                        - VIN for vehicle identification
                        - Model Year for manufacturing year
                        - Make/Model/Trim for vehicle hierarchy
                        - Body Type, Engine Size, Transmission for specs
                        - Region, Doors, Seats, Cylinders for attributes
                        """)
        else:
            # Manual column mapping (original functionality)
            st.write("**Map columns for comparison across 11 dimensions:**")
            
            # Dimensions to map
            dimensions = [
                ('ModelYear', 'Model Year'),
                ('Make', 'Make/Manufacturer'),
                ('Model', 'Model'),
                ('Trim', 'Trim Level'),
                ('BodyType', 'Body Type'),
                ('EngineSize', 'Engine Size'),
                ('Transmission', 'Transmission'),
                ('Region', 'Region'),
                ('Doors', 'Number of Doors'),
                ('Seats', 'Number of Seats'),
                ('Cylinders', 'Number of Cylinders')
            ]

            # Create column mappings
            old_column_mappings = {'VIN': st.selectbox("VIN column (Old):", old_columns, key="onv_old_vin")}
            new_column_mappings = {'VIN': st.selectbox("VIN column (New):", new_columns, key="onv_new_vin")}

            # Create expandable sections for each dimension group
            with st.expander("🚗 Basic Information (ModelYear, Make, Model)", expanded=True):
                for dimension, display_name in dimensions[:3]:
                    col1, col2 = st.columns(2)
                    with col1:
                        old_column_mappings[dimension] = st.selectbox(
                            f"{display_name} (Old):", old_columns, 
                            key=f"onv_old_{dimension.lower()}"
                        )
                    with col2:
                        new_column_mappings[dimension] = st.selectbox(
                            f"{display_name} (New):", new_columns, 
                            key=f"onv_new_{dimension.lower()}"
                        )

            with st.expander("🎨 Style & Features (Trim, BodyType, EngineSize, Transmission)"):
                for dimension, display_name in dimensions[3:7]:
                    col1, col2 = st.columns(2)
                    with col1:
                        old_column_mappings[dimension] = st.selectbox(
                            f"{display_name} (Old):", old_columns, 
                            key=f"onv_old_{dimension.lower()}"
                        )
                    with col2:
                        new_column_mappings[dimension] = st.selectbox(
                            f"{display_name} (New):", new_columns, 
                            key=f"onv_new_{dimension.lower()}"
                        )

            with st.expander("🌍 Physical Attributes (Region, Doors, Seats, Cylinders)"):
                for dimension, display_name in dimensions[7:]:
                    col1, col2 = st.columns(2)
                    with col1:
                        old_column_mappings[dimension] = st.selectbox(
                            f"{display_name} (Old):", old_columns, 
                            key=f"onv_old_{dimension.lower()}"
                        )
                    with col2:
                        new_column_mappings[dimension] = st.selectbox(
                            f"{display_name} (New):", new_columns, 
                            key=f"onv_new_{dimension.lower()}"
                        )

        # Analysis execution
        st.subheader("🔍 Multi-Dimensional Analysis")
        
        if st.button("🚀 Start Comparison Analysis", key="onv_analyze"):
            with st.spinner("Performing multi-dimensional comparison..."):
                try:
                    results = validator.perform_multi_dimensional_comparison(
                        old_column_mappings, new_column_mappings
                    )

                    st.success("✅ Analysis completed!")

                    # Display summary statistics
                    st.subheader("📊 Comparison Results")
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric("Total Records Compared", results['total_records'])
                    with col2:
                        st.metric("Records with Mismatches", results['overall_mismatches_count'])

                    # Display dimension-wise results
                    st.write("**Dimension-wise Match Statistics:**")
                    
                    # Create metrics display in a grid
                    dimension_results = results['dimension_results']
                    
                    # Group dimensions for better display
                    basic_dims = ['ModelYear', 'Make', 'Model']
                    style_dims = ['Trim', 'BodyType', 'EngineSize', 'Transmission']
                    physical_dims = ['Region', 'Doors', 'Seats', 'Cylinders']
                    
                    # Basic Information metrics
                    st.write("**Basic Information:**")
                    cols = st.columns(len(basic_dims))
                    for i, dim in enumerate(basic_dims):
                        if dim in dimension_results:
                            stats = dimension_results[dim]
                            with cols[i]:
                                st.metric(
                                    dim,
                                    f"{stats['matches']}/{stats['total']}",
                                    f"{stats['percentage']:.1f}%"
                                )
                    
                    # Style & Features metrics
                    st.write("**Style & Features:**")
                    cols = st.columns(len(style_dims))
                    for i, dim in enumerate(style_dims):
                        if dim in dimension_results:
                            stats = dimension_results[dim]
                            with cols[i]:
                                st.metric(
                                    dim,
                                    f"{stats['matches']}/{stats['total']}",
                                    f"{stats['percentage']:.1f}%"
                                )
                    
                    # Physical Attributes metrics
                    st.write("**Physical Attributes:**")
                    cols = st.columns(len(physical_dims))
                    for i, dim in enumerate(physical_dims):
                        if dim in dimension_results:
                            stats = dimension_results[dim]
                            with cols[i]:
                                st.metric(
                                    dim,
                                    f"{stats['matches']}/{stats['total']}",
                                    f"{stats['percentage']:.1f}%"
                                )

                    # Show sample mismatches
                    if results['overall_mismatches_count'] > 0:
                        st.subheader("🔍 Sample Mismatches")
                        sample_mismatches = validator.get_sample_mismatches(5)
                        if len(sample_mismatches) > 0:
                            safe_display_dataframe(sample_mismatches, 5, "Sample Mismatches")

                    # Generate and offer download
                    st.subheader("📥 Download Results")
                    
                    report = validator.save_results()
                    create_download_link(
                        report,
                        f"old_new_comparison_analysis_{int(time.time())}.xlsx",
                        "📥 Download Analysis Report"
                    )
                    
                    st.info("""
                    **📋 Output Structure:**
                    - **Analysis Sheet**: Side-by-side old/new comparison with conditional formatting
                    - **Summary Sheet**: Dimension-wise match statistics  
                    - **Mismatches Sheet**: Records with discrepancies in any dimension
                    - **Mask Sheet**: Dynamic conditional formatting formulas (11 columns A-K)
                    
                    **🎨 Color Coding**: Green = matches, Pink = mismatches across all dimensions
                    """)

                except Exception as e:
                    st.error(f"❌ Error during analysis: {str(e)}")

    else:
        st.info("Please upload both old and new data files to proceed.")


def main():
    """Main application function"""
    # Initialize session state
    if 'vv_data_prepared' not in st.session_state:
        st.session_state.vv_data_prepared = False

    display_header()

    # Sidebar navigation
    st.sidebar.markdown("### 🧭 Navigation")
    selected_workflow = st.sidebar.selectbox(
        "Select Workflow:",
        [
            "🏠 Home",
            "📊 Discrepancy Checker",
            "📈 Discrepancy Analyzer",
            "🎯 Specification Mapper",
            "🔍 Vehicle Data Verifier",
            "🔄 Old/New Data Validator",
            "🌐 Translation Service",
            "🔗 Combined Verifier & Checker"
        ]
    )

    # Add workflow descriptions in sidebar
    st.sidebar.markdown("### 📖 Workflow Descriptions")

    if selected_workflow == "🏠 Home":
        st.markdown("""
        ## Welcome to the Vehicle Data Analysis Suite! 🚗
        
        This comprehensive platform provides seven powerful workflows for vehicle data processing:
        
        ### 📊 Discrepancy Checker
        - **Purpose**: Excel/CSV processor with conditional formatting
        - **Features**: Single/two-file mode, compare columns within thresholds, join data by chassis numbers
        - **Output**: Formatted Excel files with highlighted discrepancies
        
        ### 📈 Discrepancy Analyzer
        - **Purpose**: Vehicle valuation discrepancy reporting
        - **Features**: Detailed analysis by brand, model year, and price range
        - **Output**: Comprehensive Excel reports with multiple analysis sheets
        
        ### 🎯 Specification Mapper
        - **Purpose**: Fuzzy matching for vehicle specifications
        - **Features**: Advanced mapping with Mercedes-Benz and BMW special handling
        - **Output**: Mapped and unmatched data files with consolidated results
        
        ### 🔍 Vehicle Data Verifier
        - **Purpose**: Data verification with optional translation
        - **Features**: Arabic translation support and conditional formatting
        - **Output**: Verification reports with match statistics
        
        ### 🔄 Old/New Data Validator
        - **Purpose**: Multi-dimensional comparison between old and new vehicle data files
        - **Features**: 11-dimension analysis with VIN-based matching and proper suffix conventions
        - **Output**: Excel analysis with conditional formatting across all dimensions
        
        ### 🌐 Translation Service
        - **Purpose**: Standalone Arabic to English translation
        - **Features**: Multi-column translation with progress tracking and caching
        - **Output**: Translated Excel/CSV files with before/after comparison
        
        ### 🔗 Combined Verifier & Checker
        - **Purpose**: Comprehensive analysis combining vehicle verification with discrepancy checking
        - **Features**: Two-file verification, value discrepancy analysis, optional Arabic translation, match columns toggle
        - **Output**: Single Excel report with verification results, discrepancy analysis, and dual conditional formatting
        
        ---
        
        ### 🚀 Getting Started
        1. Select a workflow from the sidebar
        2. Upload your data files
        3. Configure columns and parameters
        4. Run the analysis
        5. Download your results
        
        ### 💡 Tips
        - All workflows support Excel (.xlsx, .xls) and CSV files
        - Sheet selection is available for multi-sheet Excel files
        - Results are provided as downloadable files
        - Progress indicators show analysis status
        
        **Ready to start? Choose a workflow from the sidebar!**
        """)

    elif selected_workflow == "📊 Discrepancy Checker":
        st.sidebar.markdown("""
        **Discrepancy Checker** helps you identify data discrepancies by:
        - Single or two-file mode with chassis-based joining
        - Comparing two columns within a percentage threshold
        - Checking if values fall within specified ranges
        - Applying conditional formatting for easy visualization
        """)
        discrepancy_checker_tab()

    elif selected_workflow == "📈 Discrepancy Analyzer":
        st.sidebar.markdown("""
        **Discrepancy Analyzer** generates comprehensive reports with:
        - Brand-specific discrepancy analysis
        - Model year trend analysis
        - Price range impact assessment
        - Statistical summaries and visualizations
        """)
        discrepancy_analyzer_tab()

    elif selected_workflow == "🎯 Specification Mapper":
        st.sidebar.markdown("""
        **Specification Mapper** provides advanced fuzzy matching for:
        - Make, model, and trim standardization
        - Special handling for Mercedes-Benz and BMW
        - Configurable similarity thresholds
        - Detailed mapping statistics
        """)
        spec_mapper_tab()

    elif selected_workflow == "🔍 Vehicle Data Verifier":
        st.sidebar.markdown("""
        **Vehicle Data Verifier** offers verification with:
        - Reference vs. logs data comparison
        - Optional Arabic translation support
        - Match statistics and analysis
        - Conditional formatting for results
        """)
        vehicle_verifier_tab()

    elif selected_workflow == "🔄 Old/New Data Validator":
        st.sidebar.markdown("""
        **Old/New Data Validator** provides multi-dimensional comparison with:
        - 11-dimension analysis (ModelYear, Make, Model, Trim, etc.)
        - VIN-based matching between old and new data files
        - Proper suffix conventions (_old vs standard names)
        - Conditional formatting with color-coded results
        - Comprehensive Excel reports with multiple sheets
        """)
        old_new_validator_tab()

    elif selected_workflow == "🌐 Translation Service":
        st.sidebar.markdown("""
        **Translation Service** provides standalone translation with:
        - Upload files with Arabic text in any columns
        - Multi-column selection for translation
        - Progress tracking and translation caching
        - Before/after comparison results
        - Excel and CSV download options
        """)
        translation_service_tab()

    elif selected_workflow == "🔗 Combined Verifier & Checker":
        st.sidebar.markdown("""
        **Combined Verifier & Checker** provides comprehensive analysis with:
        - Vehicle data verification between reference and logs files
        - Value discrepancy analysis with multiple comparison modes
        - Optional Arabic translation support
        - Match columns toggle (in main sheet or separate mask sheet)
        - Conditional formatting for both verification and discrepancy results
        - Single comprehensive Excel report with multiple analysis layers
        """)
        combined_verifier_checker_tab()

    # Footer
    st.markdown("---")
    st.markdown("""
    <div style="text-align: center; color: #666; margin-top: 50px;">
        <p>Vehicle Data Analysis Suite | Built with Streamlit</p>
    </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()
