# Vehicle Data Analysis Suite

A comprehensive Streamlit-based web application for vehicle data processing, analysis, and verification.

## Features

### 1. Discrepancy Checker
Excel/CSV processor with conditional formatting for discrepancy checking. Compare two columns within a percentage threshold or check if values fall within a range.

**Features:**
- Single file or two-file mode (join data from separate files)
- Multiple analysis modes:
  - Compare two columns within % threshold
  - Absolute % difference with color coding
  - Check value between low & high columns
- Configurable dividend column selection
- Highlighting options (entire row or specific columns)

### 2. Discrepancy Analyzer
Generate comprehensive vehicle valuation discrepancy reports with detailed analysis by brand, model year, and price range.

**Features:**
- Single file or two-file mode
- Precomputed percentage difference support
- Dynamic threshold configuration
- Comprehensive reporting by vehicle attributes

### 3. Specification Mapper
Advanced fuzzy matching for vehicle specifications with special handling for Mercedes-Benz and BMW. Maps make, model, and trim data to a standardized format.

**Features:**
- Hierarchical mapping (Makes → Models → Trims)
- Brand-specific processing for Mercedes-Benz and BMW
- Configurable similarity thresholds
- Multiple fuzzy matching methods
- Option to use original values when no mapping found
- Detailed mapping reports (mapped, unmatched, consolidated data)

### 4. Vehicle Data Verifier
Verify vehicle data between reference and logs with optional Arabic translation support. Provides detailed match analysis with conditional formatting.

**Features:**
- Data verification between reference and logs
- Optional Arabic-to-English translation (requires OpenAI API key)
- Match analysis for Make, Model, and ModelYear
- Configurable output (include match columns in main sheet or separate)
- Visual conditional formatting (green for matches, red for mismatches)

### 5. Combined Verifier & Checker
Comprehensive analysis combining vehicle data verification with discrepancy checking. Performs verification and then analyzes value discrepancies.

**Features:**
- All features from Vehicle Data Verifier
- All features from Discrepancy Checker
- Seamless workflow integration

### 6. Translation Service
Standalone Arabic to English translation service for vehicle specifications using OpenAI GPT models.

**Features:**
- Multi-column translation support
- Progress tracking
- Translation caching
- Batch processing with concurrent requests

## Installation

1. **Install Python 3.8 or higher**

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

## Usage

1. **Run the application:**
   ```bash
   streamlit run streamlit_app.py
   ```

2. **Access the web interface:**
   - The application will open in your default browser
   - Default URL: http://localhost:8501

3. **Select a workflow:**
   - Use the sidebar to select the desired workflow
   - Upload your data files
   - Configure the analysis parameters
   - Process and download results

## Configuration

### File Formats Supported
- Excel (.xlsx, .xls)
- CSV (.csv)

### Column Mapping
Each workflow allows you to map your data columns to the expected format:
- Select sheets from multi-sheet Excel files
- Choose specific columns for analysis
- Configure join keys for two-file modes

### Analysis Parameters

#### Discrepancy Checker
- **Threshold**: Percentage threshold for discrepancy detection (0-100%)
- **Dividend Column**: Choose which column to use as denominator
- **Highlighting Mode**: Entire row or specific columns

#### Specification Mapper
- **Make Threshold**: Similarity threshold for make matching (default: 80%)
- **Model Threshold**: Similarity threshold for model matching (default: 80%)
- **Trim Threshold**: Similarity threshold for trim matching (default: 80%)
- **Skip Trim**: Option to skip trim mapping
- **Use Original on No Match**: Use original values when no mapping found
- **Fuzzy Methods**: Different matching algorithms for each dimension

#### Vehicle Verifier
- **Translation**: Optional Arabic translation (requires OpenAI API key)
- **Match Columns**: Include match columns in main sheet or separate

## Data Processing Pipeline

### Discrepancy Analysis Flow
```
Upload Data → Select Columns → Configure Threshold → Process → Download Results
```

### Specification Mapping Flow
```
Upload Input & Reference Data → Select Columns → Configure Thresholds → Map Specifications → Download Mapped, Unmatched, and Consolidated Data
```

### Vehicle Verification Flow
```
Upload Reference & Logs Data → Optional Translation → Verify Data → Download Analysis Report
```

## Output Files

### Discrepancy Checker
- Single Excel file with conditional formatting
- Highlighted discrepancies based on configured thresholds

### Specification Mapper
- `mapped_makes.csv`: Successfully matched makes
- `mapped_models.csv`: Successfully matched models
- `mapped_trims.csv`: Successfully matched trims
- `unmatched_makes.csv`: Unmatched makes with best candidates
- `unmatched_models.csv`: Unmatched models with best candidates
- `unmatched_trims.csv`: Unmatched trims with best candidates
- `consolidated_data.csv`: Original data with mapped columns
- `mapping_summary.csv`: Performance statistics

### Vehicle Verifier
- Excel file with multiple sheets:
  - `Analysis`: Main verification results with conditional formatting
  - `Summary`: Match statistics and percentages
  - `Mask` (optional): Match indicators for each record

## Advanced Features

### Mercedes-Benz Special Processing
Automatically extracts class and trim from model designations:
- Examples: "A 200" → "A-Class | 200", "GLC 300" → "GLC-Class | 300"

### BMW Special Processing
Automatically extracts series and trim from model designations:
- Examples: "320i" → "3 Series | 320i", "X5 xDrive35i" → "X5 Series | xDrive35i"

### Translation Service
- Uses OpenAI GPT models for accurate translation
- Supports batch processing with concurrent requests
- Maintains translation cache for efficiency
- Progress tracking for large datasets

## Troubleshooting

### PyArrow Display Issues
The application includes multiple fallback methods for displaying DataFrames:
1. Cleaned DataFrame display
2. String conversion fallback
3. st.table fallback
4. Text display fallback

### Translation Errors
- Ensure you have a valid OpenAI API key
- Check your internet connection
- Verify the `openai` library is installed: `pip install openai`

### File Loading Errors
- Ensure file formats are supported (.xlsx, .xls, .csv)
- Check file encoding (UTF-8 recommended for CSV)
- Verify file is not corrupted or password-protected

## System Requirements

- Python 3.8+
- 4GB RAM minimum (8GB recommended for large datasets)
- Modern web browser (Chrome, Firefox, Edge, Safari)
- Internet connection (for translation features)

## Dependencies

- streamlit: Web application framework
- pandas: Data manipulation and analysis
- openpyxl: Excel file handling
- thefuzz: Fuzzy string matching
- python-Levenshtein: Fast string similarity
- tqdm: Progress bars
- openai: Translation service (optional)
- python-dotenv: Environment variable management
- nest-asyncio: Async processing support
- aiohttp: Async HTTP requests

## License

This project is proprietary software for internal use.

## Support

For issues or questions, please contact the development team.

## Version

Version: 1.0.0
Last Updated: 2025-12-05
