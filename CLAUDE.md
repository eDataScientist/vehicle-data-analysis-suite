# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Vehicle Data Analysis Suite - A Streamlit-based web application for vehicle data processing, analysis, and verification. Supports Excel/CSV file processing with conditional formatting, fuzzy matching, discrepancy checking, and Arabic-to-English translation for vehicle specifications.

## Running the Application

```bash
# Install dependencies
pip install -r requirements.txt

# Run the Streamlit app
streamlit run streamlit_app.py

# Access at: http://localhost:8501
```

## Architecture

### Core Structure

The application follows a modular workflow-based architecture:

```
streamlit_app.py          # Main Streamlit UI and orchestration
├── scrips/
│   ├── workflows/        # Business logic modules (data processing workflows)
│   │   ├── discrepancy_checker.py       # Excel/CSV discrepancy analysis with conditional formatting
│   │   ├── discrepancy_analyzer.py      # Comprehensive valuation discrepancy reports
│   │   ├── spec_mapper.py               # Fuzzy matching for vehicle specs (Make/Model/Trim)
│   │   ├── vehicle_verifier.py          # Vehicle data verification between reference and logs
│   │   ├── combined_verifier_checker.py # Combines verification + discrepancy checking
│   │   └── old_new_validator.py         # Data validation workflow
│   └── services/         # Shared services
│       └── translation_service.py       # OpenAI-based Arabic translation with async support
```

### Workflow Pattern

Each workflow class follows this structure:
1. **Data Loading**: `load_*_data()` methods return available sheets
2. **Sheet Selection**: `select_*_sheet()` methods load specific sheets into DataFrames
3. **Column Access**: `get_*_columns()` methods return available columns
4. **Processing**: Main processing method that returns results
5. **Export**: Methods to generate Excel/CSV outputs with conditional formatting

### Key Workflows

**DiscrepancyChecker**
- Single-file or two-file mode (with join operations)
- Compare columns within percentage thresholds
- Conditional formatting using openpyxl (FormulaRule, PatternFill)
- Supports highlighting entire rows or specific columns

**SpecMapper**
- Hierarchical fuzzy matching: Makes → Models → Trims
- Special processing for Mercedes-Benz (extracts class/trim from model)
- Special processing for BMW (extracts series/trim from model)
- Uses thefuzz library with configurable thresholds
- Outputs: mapped, unmatched, and consolidated CSV files

**VehicleDataVerifier**
- Verifies Make, Model, ModelYear between reference and logs
- Optional Arabic-to-English translation via OpenAI
- Conditional formatting for matches (green) and mismatches (red)
- Match columns can be in main sheet or separate sheet

**CombinedVerifierChecker**
- Combines VehicleDataVerifier + DiscrepancyChecker
- Preserves full original data before verification
- Seamless workflow integration

**TranslationService**
- Async batch translation using OpenAI AsyncOpenAI client
- Rate limiting with semaphore control
- Translation caching to avoid duplicate API calls
- Automotive-specific system prompts
- Uses nest_asyncio for Jupyter/Streamlit compatibility

## Environment Variables

Required for translation features:
```
OPENAI_API_KEY=<your_key>
GEMINI_API_KEY=<your_key>  # Currently unused but configured
```

Store in `.env` file (git-ignored).

## Dependencies

Key libraries:
- `streamlit`: Web interface
- `pandas`: Data manipulation
- `openpyxl`: Excel file handling and conditional formatting
- `thefuzz` + `python-Levenshtein`: Fuzzy string matching
- `openai`: Translation via GPT models (optional)
- `nest-asyncio`: Async support in Jupyter/Streamlit environments
- `tqdm`: Progress bars for long-running operations

## File Handling Standards

- **Encoding**: Always use `encoding='utf-sig'` when saving CSV files with pandas
- **Supported Formats**: Excel (.xlsx, .xls) and CSV (.csv)
- **Excel Sheets**: Multi-sheet support via `pd.ExcelFile()` and sheet selection
- **Conditional Formatting**: Applied using openpyxl after DataFrame export

## Brand-Specific Processing

### Mercedes-Benz
Automatically extracts class and trim from model strings:
- "A 200" → "A-Class | 200"
- "GLC 300" → "GLC-Class | 300"

### BMW
Automatically extracts series and trim from model strings:
- "320i" → "3 Series | 320i"
- "X5 xDrive35i" → "X5 Series | xDrive35i"

## Translation Implementation

When adding translation features:
1. Import `ArabicTranslationService` from `scrips.services.translation_service`
2. Use async methods with `nest_asyncio` for Streamlit compatibility
3. Provide progress callbacks for user feedback
4. Use automotive-specific system prompts for context
5. Handle ImportError gracefully (translation is optional)

## Common Patterns

### Two-File Mode with Join
Many workflows support joining data from two files:
1. Load primary file and select sheet
2. Load join file and select sheet
3. Specify join columns (keys)
4. Merge DataFrames using pandas merge operations

### Conditional Formatting
Applied post-export using openpyxl:
1. Export DataFrame to Excel
2. Load workbook with openpyxl
3. Apply FormulaRule or CellIsRule with PatternFill
4. Save workbook to BytesIO for download

### Progress Tracking
Use `tqdm` for long operations:
- Fuzzy matching iterations
- Row-by-row processing
- Batch translation requests

## Important Notes

- This repository is NOT a git repository (no .git directory)
- Virtual environment located in `venv/` directory
- Main entry point is `streamlit_app.py` with workflow selection in sidebar
- All workflows return BytesIO objects for Streamlit download buttons
- PyArrow display issues are handled with multiple fallback methods in the Streamlit app
