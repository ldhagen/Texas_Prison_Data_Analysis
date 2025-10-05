# Texas Prison Data Analyzer

A Dockerized PHP/Python solution for comparing large Texas Prison datasets with memory-efficient processing, interactive record review, sortable tables, and detailed record modals.

## Features

- **Memory-Efficient Processing**: Converts large pickle files to Parquet format
- **Interactive Web Interface**: Browse, compare, and analyze datasets
- **Full Dataset Sorting**: Sort entire dataset by any column, not just visible page
- **Clickable Records**: Click any row to view complete record details in modal
- **Dataset Comparison**: Identify differences between snapshots over time
- **Paginated Browser**: Navigate through thousands of records efficiently
- **Dockerized**: Easy deployment with all dependencies included

## Quick Start

### 1. Setup Project Structure

```bash
# Create project directory
mkdir prison-data-analyzer
cd prison-data-analyzer

# Create subdirectories
mkdir -p src data cache exports

# Copy the required files:
# - Dockerfile (root directory)
# - docker-compose.yml (root directory)
# - src/index.php
# - src/pkl_extractor.py
```

### 2. Copy Your Data Files

```bash
# Copy all .pkl files from your current location
cp /var/tmp/ldh/Prison_Data/*.pkl ./data/

# Verify files are copied
ls -lh ./data/
```

### 3. Fix Permissions

```bash
# Set permissions for cache and exports directories
chmod -R 777 cache/
chmod -R 777 exports/

# Make Python script executable
chmod +x src/pkl_extractor.py
```

### 4. Build and Start Docker Container

```bash
# Build the container (takes 5-10 minutes due to pandas compilation)
docker-compose build

# Start the application
docker-compose up -d

# Check status
docker-compose ps
```

### 5. Access the Application

Open your browser and navigate to:
```
http://localhost:8080
```

## Usage Guide

### Converting Pickle Files

1. Go to the **Files** tab
2. Click **"View Metadata"** to see file information:
   - Number of rows
   - Column names
   - Memory usage
   - Data types
3. Click **"Convert to Parquet"** to process each file
4. Wait for conversion (1-2 minutes per 20MB file)
5. Converted files appear in "Converted Parquet Files" section

**Note**: Convert all your pickle files before using Compare or Browse features.

### Comparing Datasets

1. Go to the **Compare** tab
2. Select two parquet files from the dropdowns (e.g., Sep 2024 vs Oct 2025)
3. Click **"Compare"**
4. View comprehensive statistics:
   - Total rows in each file
   - Row difference between files
   - Records only in File 1
   - Records only in File 2
   - Common records between files
   - ID column used for comparison

**Use Cases**:
- Track population changes over time
- Identify new inmates vs released inmates
- Monitor facility transfers
- Detect data quality issues

### Browsing Records

1. Go to the **Browse Records** tab
2. Select a parquet file from dropdown
3. Click **"Load Records"**
4. Wait for initial load (all records loaded for sorting)
   - Progress indicator shows: "Loading records... 75% complete"
   - For 20,000 records: ~5-10 seconds
5. **Sort Data**: Click any column header to sort entire dataset
   - First click: ascending (▲)
   - Second click: descending (▼)
   - Sorts ALL records, not just visible page
6. **View Details**: Click any row to open detailed modal
   - Shows all fields in clean layout
   - Auto-detects record identifier for title
   - Close with X, button, ESC key, or click outside
7. **Navigate**: Use Previous/Next buttons (50 records per page)

**Features**:
- Instant pagination after initial load
- Click any column header to sort entire dataset
- Click any row for full record details
- Visual hover effects for better UX
- Keyboard shortcuts (ESC to close modal)

## Memory Efficiency

The system uses several techniques to handle large datasets:

1. **Parquet Format**: Converts pickle to columnar format with Snappy compression
2. **Chunked Loading**: Loads records in 1000-record chunks to avoid timeouts
3. **In-Memory Caching**: After initial load, sorting and pagination are instant
4. **Garbage Collection**: Explicitly frees memory after operations
5. **Efficient Storage**: Parquet files are typically same size or smaller than pickle

## File Structure

```
prison-data-analyzer/
├── Dockerfile              # Container configuration
├── docker-compose.yml      # Docker compose setup
├── README.md              # This file
├── src/
│   ├── index.php          # Main PHP application
│   └── pkl_extractor.py   # Python data processing
├── data/                  # Your .pkl files (input)
│   ├── 14_Sep_24_Texas_Prison.pkl
│   ├── 01_Dec_24_12_09_30_36_Texas_Prison.pkl
│   └── ...
├── cache/                 # Converted .parquet files (auto-created)
└── exports/               # Export location (auto-created)
```

## Dataset Information

Your datasets appear to be monthly snapshots of Texas Prison data:
- **14_Sep_24_Texas_Prison.pkl** (Sep 2024) - 21.0 MB
- **01_Dec_24_12_09_30_36_Texas_Prison.pkl** (Dec 2024) - 20.9 MB
- **01_Apr_25_04_09_30_32_Texas_Prison.pkl** (Apr 2025) - 21.2 MB
- **01_Jun_25_06_09_30_56_Texas_Prison.pkl** (Jun 2025) - 21.3 MB
- **01_Jul_25_07_09_30_37_Texas_Prison.pkl** (Jul 2025) - 21.4 MB
- **02_Jul_25_07_09_31_18_Texas_Prison.pkl** (Jul 2025 - v2) - 21.4 MB
- **01_Aug_25_08_09_30_29_Texas_Prison.pkl** (Aug 2025) - 21.5 MB
- **01_Oct_25_10_09_30_20_Texas_Prison.pkl** (Oct 2025) - 21.8 MB

Total: ~170 MB of data spanning 13 months

## Docker Commands

```bash
# Start container
docker-compose up -d

# Stop container
docker-compose down

# View logs
docker-compose logs -f web

# Restart container (after code changes)
docker-compose restart

# Rebuild after Dockerfile changes
docker-compose up -d --build

# Remove everything (including volumes)
docker-compose down -v

# Enter container shell (for debugging)
docker exec -it prison_data_analyzer bash

# Check container status
docker-compose ps
```

## Troubleshooting

### Files not showing up
```bash
# Verify file permissions
ls -la ./data/
chmod 644 ./data/*.pkl

# Check if files are visible in container
docker exec -it prison_data_analyzer ls -la /var/www/html/data/
```

### Permission Denied Errors
```bash
# Fix cache and exports permissions
chmod -R 777 cache/
chmod -R 777 exports/

# Or fix inside container
docker exec -it prison_data_analyzer chmod -R 777 /var/www/html/cache
docker exec -it prison_data_analyzer chmod -R 777 /var/www/html/exports

# Restart container
docker-compose restart
```

### Python Errors
```bash
# Check Python environment in container
docker exec -it prison_data_analyzer /opt/venv/bin/python3 --version

# Check installed packages
docker exec -it prison_data_analyzer /opt/venv/bin/pip list

# Test Python script manually
docker exec -it prison_data_analyzer /opt/venv/bin/python3 /var/www/html/pkl_extractor.py metadata /var/www/html/data/14_Sep_24_Texas_Prison.pkl
```

### Memory Issues
```bash
# Increase Docker memory limit in Docker Desktop settings
# Recommended: 4GB+ RAM for large datasets

# Check current memory usage
docker stats prison_data_analyzer
```

### Can't Access localhost:8080
```bash
# Check if container is running
docker-compose ps

# Check logs for errors
docker-compose logs web

# Verify port is not in use
lsof -i :8080  # Linux/Mac
netstat -ano | findstr :8080  # Windows

# Try alternative port (edit docker-compose.yml)
# Change "8080:80" to "8081:80"
```

### Slow Conversion or Loading
```bash
# This is normal for large files
# Monitor progress in logs
docker-compose logs -f web

# Initial record load takes 5-10 seconds for 20k records
# Subsequent sorts and pagination are instant
```

### Build Fails (Pandas Compilation)
```bash
# Clean and retry
docker-compose down
docker system prune -f
docker-compose build --no-cache

# Build takes 5-10 minutes due to pandas compilation
# Be patient and watch for errors
```

## Advanced Features

### Manual Python Commands

You can run Python commands directly for debugging:

```bash
# Get metadata
docker exec -it prison_data_analyzer /opt/venv/bin/python3 \
  /var/www/html/pkl_extractor.py metadata \
  /var/www/html/data/14_Sep_24_Texas_Prison.pkl

# Convert file
docker exec -it prison_data_analyzer /opt/venv/bin/python3 \
  /var/www/html/pkl_extractor.py convert \
  /var/www/html/data/14_Sep_24_Texas_Prison.pkl \
  /var/www/html/cache

# Compare datasets
docker exec -it prison_data_analyzer /opt/venv/bin/python3 \
  /var/www/html/pkl_extractor.py compare \
  /var/www/html/cache/file1.parquet \
  /var/www/html/cache/file2.parquet

# Load specific records
docker exec -it prison_data_analyzer /opt/venv/bin/python3 \
  /var/www/html/pkl_extractor.py load_chunk \
  /var/www/html/cache/file1.parquet 0 100
```

### Backup Parquet Files

```bash
# Copy converted files out of container
docker cp prison_data_analyzer:/var/www/html/cache ./backup/

# Or use volume mapping (already configured in docker-compose.yml)
cp ./cache/*.parquet ./backup/

# Create timestamped backup
tar -czf prison-data-backup-$(date +%Y%m%d).tar.gz cache/
```

### Access Container Files

```bash
# View cache directory
docker exec -it prison_data_analyzer ls -lh /var/www/html/cache/

# View data directory
docker exec -it prison_data_analyzer ls -lh /var/www/html/data/

# Copy file from container
docker cp prison_data_analyzer:/var/www/html/cache/file.parquet ./
```

## Performance Notes

- **Initial build**: 5-10 minutes (pandas compilation)
- **File conversion**: 1-2 minutes per 20MB pickle file
- **Initial record load**: 5-10 seconds for 20,000 records (loads all for sorting)
- **Sorting**: < 1 second (in-memory operation)
- **Pagination**: Instant (after initial load)
- **Record modal**: Instant
- **Comparison**: 2-5 seconds for files with 100k+ records
- **Memory usage**: ~512MB per operation, up to 2GB for very large datasets

## Analysis Capabilities

With your 9 datasets spanning **Sep 2024 to Oct 2025**, you can:

✅ **Track Population Trends**: Compare total inmate counts over time  
✅ **Identify New Inmates**: Find records only in recent files  
✅ **Track Releases**: Find records only in older files  
✅ **Monitor Transfers**: Compare facility assignments across months  
✅ **Data Quality**: Spot inconsistencies or missing data  
✅ **Individual Tracking**: Follow specific inmates across time  
✅ **Facility Analysis**: Sort by facility to analyze specific locations  
✅ **Temporal Analysis**: Compare any two snapshots to see changes

## Security Notes

This application is designed for **local/internal use only**. For production deployment:

- ⚠️ Add authentication/authorization
- ⚠️ Validate and sanitize all user inputs
- ⚠️ Use environment variables for sensitive configuration
- ⚠️ Restrict file system access
- ⚠️ Add HTTPS support with SSL certificates
- ⚠️ Implement rate limiting
- ⚠️ Add audit logging for data access
- ⚠️ Ensure compliance with data privacy regulations (HIPAA, etc.)

## Data Privacy

⚠️ **Important**: Prison records may contain sensitive personal information. Ensure you:

- Have proper authorization to access and analyze this data
- Comply with all relevant data protection regulations
- Implement appropriate access controls
- Do not share data with unauthorized parties
- Follow your organization's data retention policies
- Secure backups appropriately

## System Requirements

- **Docker Desktop**: 20.10 or later
- **RAM**: 4GB minimum, 8GB recommended
- **Disk Space**: 1GB for Docker images + space for your data
- **OS**: Windows 10/11, macOS 10.15+, or Linux with Docker support
- **Browser**: Modern browser (Chrome, Firefox, Edge, Safari)

## Future Enhancements

Potential features to add:

- [ ] Search functionality (filter by name, ID, facility)
- [ ] CSV export for filtered/sorted data
- [ ] Data visualizations (charts, graphs, trends)
- [ ] Advanced filtering (date ranges, multiple criteria)
- [ ] Batch conversion (convert all files at once)
- [ ] Record history view (track individual across files)
- [ ] Automated comparison reports
- [ ] API endpoints for programmatic access
- [ ] User authentication and roles
- [ ] Audit logging

## License

For internal use. Ensure compliance with data privacy regulations when handling prison records.

## Support

For issues, questions, or feature requests:

1. Check this README troubleshooting section
2. Review Docker logs: `docker-compose logs -f`
3. Verify file permissions and paths
4. Test Python script manually (see Advanced Features)
5. Check Docker resource allocation

## Version History

**v1.0** (Current)
- Initial release with pickle to parquet conversion
- Dataset comparison functionality
- Interactive record browser with full dataset sorting
- Clickable rows with detailed modal view
- Dockerized deployment

---

Built with PHP 8.2, Python 3, pandas, pyarrow, and Apache in Docker.