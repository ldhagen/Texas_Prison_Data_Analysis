<?php
// Prison Data Analysis System
error_reporting(E_ALL);
ini_set('display_errors', 1);
ini_set('memory_limit', '512M');

class PrisonDataAnalyzer {
    private $dataDir;
    private $cacheDir;
    private $pythonScript;
    
    public function __construct() {
        $this->dataDir = __DIR__ . '/data';
        $this->cacheDir = __DIR__ . '/cache';
        $this->pythonScript = __DIR__ . '/pkl_extractor.py';
        
        if (!file_exists($this->cacheDir)) {
            mkdir($this->cacheDir, 0755, true);
        }
    }
    
    public function executePython($command, $args = []) {
        $cmd = '/opt/venv/bin/python3 ' . escapeshellarg($this->pythonScript) . ' ' . escapeshellarg($command);
        foreach ($args as $arg) {
            $cmd .= ' ' . escapeshellarg($arg);
        }
        
        $output = shell_exec($cmd . ' 2>&1');
        return json_decode($output, true);
    }
    
    public function listPickleFiles() {
        $files = glob($this->dataDir . '/*.pkl');
        $fileList = [];
        
        foreach ($files as $file) {
            $fileList[] = [
                'name' => basename($file),
                'path' => $file,
                'size_mb' => round(filesize($file) / 1024 / 1024, 2),
                'modified' => date('Y-m-d H:i:s', filemtime($file))
            ];
        }
        
        usort($fileList, function($a, $b) {
            return strcmp($b['modified'], $a['modified']);
        });
        
        return $fileList;
    }
    
    public function getMetadata($filename) {
        $filepath = $this->dataDir . '/' . $filename;
        if (!file_exists($filepath)) {
            return ['error' => 'File not found'];
        }
        
        return $this->executePython('metadata', [$filepath]);
    }
    
    public function convertToParquet($filename) {
        $filepath = $this->dataDir . '/' . $filename;
        if (!file_exists($filepath)) {
            return ['error' => 'File not found'];
        }
        
        return $this->executePython('convert', [$filepath, $this->cacheDir]);
    }
    
    public function loadRecords($parquetFile, $start = 0, $limit = 100) {
        $filepath = $this->cacheDir . '/' . $parquetFile;
        if (!file_exists($filepath)) {
            return ['error' => 'Parquet file not found. Convert the pickle file first.'];
        }
        
        return $this->executePython('load_chunk', [$filepath, $start, $limit]);
    }
    
    public function compareDatasets($file1, $file2) {
        $path1 = $this->cacheDir . '/' . $file1;
        $path2 = $this->cacheDir . '/' . $file2;
        
        if (!file_exists($path1) || !file_exists($path2)) {
            return ['error' => 'One or both parquet files not found'];
        }
        
        return $this->executePython('compare', [$path1, $path2]);
    }
    
    public function listParquetFiles() {
        $files = glob($this->cacheDir . '/*.parquet');
        $fileList = [];
        
        foreach ($files as $file) {
            $fileList[] = [
                'name' => basename($file),
                'size_mb' => round(filesize($file) / 1024 / 1024, 2),
                'modified' => date('Y-m-d H:i:s', filemtime($file))
            ];
        }
        
        return $fileList;
    }
}

// Handle AJAX requests
if ($_SERVER['REQUEST_METHOD'] === 'POST' || isset($_GET['action'])) {
    header('Content-Type: application/json');
    $analyzer = new PrisonDataAnalyzer();
    
    $action = $_POST['action'] ?? $_GET['action'] ?? '';
    
    switch ($action) {
        case 'list_pkl':
            echo json_encode($analyzer->listPickleFiles());
            exit;
            
        case 'list_parquet':
            echo json_encode($analyzer->listParquetFiles());
            exit;
            
        case 'metadata':
            echo json_encode($analyzer->getMetadata($_POST['filename']));
            exit;
            
        case 'convert':
            echo json_encode($analyzer->convertToParquet($_POST['filename']));
            exit;
            
        case 'load_records':
            $start = intval($_POST['start'] ?? 0);
            $limit = intval($_POST['limit'] ?? 100);
            echo json_encode($analyzer->loadRecords($_POST['parquet_file'], $start, $limit));
            exit;
            
        case 'compare':
            echo json_encode($analyzer->compareDatasets($_POST['file1'], $_POST['file2']));
            exit;
    }
}

$analyzer = new PrisonDataAnalyzer();
?>
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Texas Prison Data Analyzer</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #f5f5f5; padding: 20px; }
        .container { max-width: 1400px; margin: 0 auto; }
        h1 { color: #333; margin-bottom: 30px; }
        .section { background: white; padding: 25px; margin-bottom: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .section h2 { color: #555; margin-bottom: 15px; font-size: 20px; }
        .file-list { display: grid; gap: 10px; }
        .file-item { padding: 15px; background: #f9f9f9; border-radius: 5px; border-left: 4px solid #4CAF50; }
        .file-item h3 { color: #333; font-size: 16px; margin-bottom: 5px; }
        .file-meta { color: #666; font-size: 14px; }
        button { background: #4CAF50; color: white; border: none; padding: 10px 20px; border-radius: 5px; cursor: pointer; font-size: 14px; margin-right: 10px; }
        button:hover { background: #45a049; }
        button:disabled { background: #ccc; cursor: not-allowed; }
        button.secondary { background: #2196F3; }
        button.secondary:hover { background: #0b7dda; }
        .loading { display: none; color: #2196F3; font-style: italic; }
        .error { color: #f44336; padding: 10px; background: #ffebee; border-radius: 5px; margin-top: 10px; }
        .success { color: #4CAF50; padding: 10px; background: #e8f5e9; border-radius: 5px; margin-top: 10px; }
        table { width: 100%; border-collapse: collapse; margin-top: 15px; overflow-x: auto; display: block; }
        table th, table td { padding: 12px; text-align: left; border-bottom: 1px solid #ddd; white-space: nowrap; }
        table th { background: #f5f5f5; font-weight: 600; position: sticky; top: 0; cursor: pointer; user-select: none; }
        table th:hover { background: #e8e8e8; }
        table th::after { content: ' ⇅'; color: #ccc; font-size: 12px; }
        table th.sort-asc::after { content: ' ▲'; color: #4CAF50; }
        table th.sort-desc::after { content: ' ▼'; color: #4CAF50; }
        table tbody tr { cursor: pointer; transition: background 0.2s; }
        table tbody tr:hover { background: #e3f2fd; }
        .pagination { margin-top: 15px; display: flex; gap: 10px; align-items: center; }
        .comparison-result { margin-top: 15px; padding: 15px; background: #e3f2fd; border-radius: 5px; }
        .comparison-result h3 { margin-bottom: 10px; color: #1976d2; }
        .stat { display: inline-block; margin: 5px 15px 5px 0; }
        select { padding: 8px; border: 1px solid #ddd; border-radius: 5px; margin-right: 10px; font-size: 14px; }
        .tabs { display: flex; gap: 10px; margin-bottom: 20px; border-bottom: 2px solid #ddd; }
        .tab { padding: 10px 20px; cursor: pointer; border-bottom: 3px solid transparent; transition: all 0.3s; }
        .tab.active { border-bottom-color: #4CAF50; color: #4CAF50; font-weight: 600; }
        .tab-content { display: none; }
        .tab-content.active { display: block; }
        
        /* Modal Styles */
        .modal { display: none; position: fixed; z-index: 1000; left: 0; top: 0; width: 100%; height: 100%; overflow: auto; background-color: rgba(0,0,0,0.4); animation: fadeIn 0.3s; }
        @keyframes fadeIn { from { opacity: 0; } to { opacity: 1; } }
        .modal-content { background-color: #fefefe; margin: 5% auto; padding: 0; border-radius: 8px; box-shadow: 0 4px 20px rgba(0,0,0,0.3); width: 90%; max-width: 800px; animation: slideDown 0.3s; }
        @keyframes slideDown { from { transform: translateY(-50px); opacity: 0; } to { transform: translateY(0); opacity: 1; } }
        .modal-header { padding: 20px; background: #4CAF50; color: white; border-radius: 8px 8px 0 0; display: flex; justify-content: space-between; align-items: center; }
        .modal-header h2 { margin: 0; font-size: 24px; }
        .modal-body { padding: 20px; max-height: 60vh; overflow-y: auto; }
        .modal-footer { padding: 15px 20px; background: #f5f5f5; border-radius: 0 0 8px 8px; text-align: right; }
        .close { color: white; font-size: 32px; font-weight: bold; cursor: pointer; transition: transform 0.2s; }
        .close:hover, .close:focus { transform: scale(1.2); }
        .record-field { display: grid; grid-template-columns: 200px 1fr; gap: 10px; padding: 12px; border-bottom: 1px solid #eee; }
        .record-field:hover { background: #f9f9f9; }
        .record-field-label { font-weight: 600; color: #555; }
        .record-field-value { color: #333; word-break: break-word; }
        .info-box { background: #e3f2fd; padding: 15px; border-radius: 5px; margin-bottom: 20px; border-left: 4px solid #2196F3; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🔍 Texas Prison Data Analyzer</h1>
        
        <div class="info-box">
            <strong>📋 Instructions:</strong> First convert your .pkl files to Parquet format, then use Compare and Browse features.
        </div>
        
        <div class="tabs">
            <div class="tab active" onclick="switchTab('files')">📁 Files</div>
            <div class="tab" onclick="switchTab('compare')">⚖️ Compare</div>
            <div class="tab" onclick="switchTab('browse')">📊 Browse Records</div>
        </div>
        
        <!-- Files Tab -->
        <div id="files-tab" class="tab-content active">
            <div class="section">
                <h2>Pickle Files (.pkl)</h2>
                <div id="pkl-files" class="file-list">
                    <p style="color: #666;">Loading...</p>
                </div>
            </div>
            
            <div class="section">
                <h2>Converted Parquet Files</h2>
                <div id="parquet-files" class="file-list">
                    <p style="color: #666;">Loading...</p>
                </div>
            </div>
        </div>
        
        <!-- Compare Tab -->
        <div id="compare-tab" class="tab-content">
            <div class="section">
                <h2>Compare Two Datasets</h2>
                <div>
                    <select id="compare-file1"></select>
                    <select id="compare-file2"></select>
                    <button onclick="compareDatasets()">Compare</button>
                </div>
                <div id="comparison-result"></div>
            </div>
        </div>
        
        <!-- Browse Tab -->
        <div id="browse-tab" class="tab-content">
            <div class="section">
                <h2>Browse Records</h2>
                <div>
                    <select id="browse-file"></select>
                    <button onclick="loadRecords()">Load Records</button>
                </div>
                <div id="records-container"></div>
            </div>
        </div>
    </div>
    
    <!-- Record Detail Modal -->
    <div id="recordModal" class="modal">
        <div class="modal-content">
            <div class="modal-header">
                <h2 id="modalTitle">Record Details</h2>
                <span class="close" onclick="closeModal()">&times;</span>
            </div>
            <div class="modal-body" id="modalBody">
                <!-- Record details will be inserted here -->
            </div>
            <div class="modal-footer">
                <button onclick="closeModal()">Close</button>
            </div>
        </div>
    </div>

    <script>
        let currentPage = 0;
        const recordsPerPage = 50;
        let currentDataset = null;
        let sortColumn = null;
        let sortDirection = 'asc';
        let allRecords = [];
        let totalRecords = 0;
        let currentFile = null;
        
        function switchTab(tabName) {
            document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
            document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
            
            event.target.classList.add('active');
            document.getElementById(tabName + '-tab').classList.add('active');
            
            if (tabName === 'compare' || tabName === 'browse') {
                loadParquetFiles();
            }
        }
        
        async function loadPickleFiles() {
            try {
                const response = await fetch('?action=list_pkl');
                const files = await response.json();
                
                const container = document.getElementById('pkl-files');
                if (!files || files.length === 0) {
                    container.innerHTML = '<p style="color: #666;">No pickle files found in /data directory. Please copy your .pkl files there.</p>';
                    return;
                }
                
                container.innerHTML = files.map(file => `
                    <div class="file-item">
                        <h3>${file.name}</h3>
                        <div class="file-meta">
                            Size: ${file.size_mb} MB | Modified: ${file.modified}
                        </div>
                        <div style="margin-top: 10px;">
                            <button onclick="viewMetadata('${file.name}')">View Metadata</button>
                            <button class="secondary" onclick="convertFile('${file.name}')">Convert to Parquet</button>
                        </div>
                        <div id="meta-${file.name.replace(/\./g, '_')}" style="margin-top: 10px;"></div>
                    </div>
                `).join('');
            } catch (error) {
                console.error('Error loading pickle files:', error);
            }
        }
        
        async function loadParquetFiles() {
            try {
                const response = await fetch('?action=list_parquet');
                const files = await response.json();
                
                const container = document.getElementById('parquet-files');
                if (container) {
                    container.innerHTML = files.length ? files.map(file => `
                        <div class="file-item">
                            <h3>${file.name}</h3>
                            <div class="file-meta">
                                Size: ${file.size_mb} MB | Modified: ${file.modified}
                            </div>
                        </div>
                    `).join('') : '<p style="color: #666;">No parquet files yet. Convert pickle files first.</p>';
                }
                
                // Populate dropdowns
                const fileOptions = files.map(f => `<option value="${f.name}">${f.name}</option>`).join('');
                ['compare-file1', 'compare-file2', 'browse-file'].forEach(id => {
                    const el = document.getElementById(id);
                    if (el) el.innerHTML = '<option value="">Select file...</option>' + fileOptions;
                });
            } catch (error) {
                console.error('Error loading parquet files:', error);
            }
        }
        
        async function viewMetadata(filename) {
            const metaDiv = document.getElementById('meta-' + filename.replace(/\./g, '_'));
            metaDiv.innerHTML = '<p class="loading" style="display: block;">Loading metadata...</p>';
            
            try {
                const formData = new FormData();
                formData.append('action', 'metadata');
                formData.append('filename', filename);
                
                const response = await fetch('', { method: 'POST', body: formData });
                const data = await response.json();
                
                if (data.error) {
                    metaDiv.innerHTML = `<p class="error">${data.error}</p>`;
                } else {
                    metaDiv.innerHTML = `
                        <div class="success">
                            <strong>Rows:</strong> ${data.rows.toLocaleString()} | 
                            <strong>Columns:</strong> ${data.columns.length} | 
                            <strong>Memory:</strong> ${data.memory_mb.toFixed(2)} MB<br>
                            <strong>Columns:</strong> ${data.columns.join(', ')}
                        </div>
                    `;
                }
            } catch (error) {
                metaDiv.innerHTML = `<p class="error">Error: ${error.message}</p>`;
            }
        }
        
        async function convertFile(filename) {
            if (!confirm(`Convert ${filename} to Parquet format? This may take 1-2 minutes.`)) return;
            
            const metaDiv = document.getElementById('meta-' + filename.replace(/\./g, '_'));
            metaDiv.innerHTML = '<p class="loading" style="display: block;">Converting... This may take a minute.</p>';
            
            try {
                const formData = new FormData();
                formData.append('action', 'convert');
                formData.append('filename', filename);
                
                const response = await fetch('', { method: 'POST', body: formData });
                const data = await response.json();
                
                if (data.success) {
                    metaDiv.innerHTML = `<p class="success">✓ Converted! Output: ${data.output_file} (${data.size_mb.toFixed(2)} MB, ${data.rows.toLocaleString()} rows)</p>`;
                    loadParquetFiles();
                } else {
                    metaDiv.innerHTML = `<p class="error">Error: ${data.error}</p>`;
                }
            } catch (error) {
                metaDiv.innerHTML = `<p class="error">Error: ${error.message}</p>`;
            }
        }
        
        async function compareDatasets() {
            const file1 = document.getElementById('compare-file1').value;
            const file2 = document.getElementById('compare-file2').value;
            
            if (!file1 || !file2) {
                alert('Please select two files to compare');
                return;
            }
            
            if (file1 === file2) {
                alert('Please select two different files');
                return;
            }
            
            const resultDiv = document.getElementById('comparison-result');
            resultDiv.innerHTML = '<p class="loading" style="display: block;">Comparing datasets...</p>';
            
            try {
                const formData = new FormData();
                formData.append('action', 'compare');
                formData.append('file1', file1);
                formData.append('file2', file2);
                
                const response = await fetch('', { method: 'POST', body: formData });
                const data = await response.json();
                
                if (data.error) {
                    resultDiv.innerHTML = `<p class="error">${data.error}</p>`;
                } else {
                    resultDiv.innerHTML = `
                        <div class="comparison-result">
                            <h3>Comparison Results</h3>
                            <div class="stat"><strong>File 1:</strong> ${data.file1} (${data.rows_file1.toLocaleString()} rows)</div>
                            <div class="stat"><strong>File 2:</strong> ${data.file2} (${data.rows_file2.toLocaleString()} rows)</div>
                            <div class="stat"><strong>Row Difference:</strong> ${data.row_difference > 0 ? '+' : ''}${data.row_difference}</div>
                            ${data.records_only_in_file1 !== undefined ? `
                                <br>
                                <div class="stat"><strong>Only in File 1:</strong> ${data.records_only_in_file1.toLocaleString()}</div>
                                <div class="stat"><strong>Only in File 2:</strong> ${data.records_only_in_file2.toLocaleString()}</div>
                                <div class="stat"><strong>Common Records:</strong> ${data.common_records.toLocaleString()}</div>
                            ` : ''}
                        </div>
                    `;
                }
            } catch (error) {
                resultDiv.innerHTML = `<p class="error">Error: ${error.message}</p>`;
            }
        }
        
        async function loadRecords(page = 0) {
            const filename = document.getElementById('browse-file').value;
            if (!filename) {
                alert('Please select a file');
                return;
            }
            
            // If loading a new file or first time, load all records
            if (currentFile !== filename) {
                currentFile = filename;
                currentPage = 0;
                sortColumn = null;
                sortDirection = 'asc';
                await loadAllRecords(filename);
            } else {
                currentPage = page;
            }
            
            displayRecords();
        }
        
        async function loadAllRecords(filename) {
            const container = document.getElementById('records-container');
            container.innerHTML = '<p class="loading" style="display: block;">Loading all records for sorting... This may take a moment.</p>';
            
            try {
                // First get total count
                const formData = new FormData();
                formData.append('action', 'load_records');
                formData.append('parquet_file', filename);
                formData.append('start', 0);
                formData.append('limit', 1);
                
                const response = await fetch('', { method: 'POST', body: formData });
                const result = await response.json();
                
                if (result.error) {
                    container.innerHTML = `<p class="error">${result.error}</p>`;
                    return;
                }
                
                totalRecords = result.total_rows;
                
                // Load all records in chunks to avoid memory issues
                allRecords = [];
                const chunkSize = 1000;
                const totalChunks = Math.ceil(totalRecords / chunkSize);
                
                for (let i = 0; i < totalChunks; i++) {
                    const chunkFormData = new FormData();
                    chunkFormData.append('action', 'load_records');
                    chunkFormData.append('parquet_file', filename);
                    chunkFormData.append('start', i * chunkSize);
                    chunkFormData.append('limit', chunkSize);
                    
                    const chunkResponse = await fetch('', { method: 'POST', body: chunkFormData });
                    const chunkResult = await chunkResponse.json();
                    
                    if (chunkResult.success && chunkResult.data) {
                        allRecords = allRecords.concat(chunkResult.data);
                    }
                    
                    // Update progress
                    container.innerHTML = `<p class="loading" style="display: block;">Loading records... ${Math.round((i + 1) / totalChunks * 100)}% complete</p>`;
                }
                
                currentDataset = { total_rows: totalRecords, data: allRecords };
            } catch (error) {
                container.innerHTML = `<p class="error">Error: ${error.message}</p>`;
            }
        }
        
        function displayRecords() {
            const container = document.getElementById('records-container');
            
            if (!allRecords || allRecords.length === 0) {
                container.innerHTML = '<p>No records found</p>';
                return;
            }
            
            const columns = Object.keys(allRecords[0]);
            const totalPages = Math.ceil(allRecords.length / recordsPerPage);
            const start = currentPage * recordsPerPage;
            const end = Math.min(start + recordsPerPage, allRecords.length);
            const pageData = allRecords.slice(start, end);
            
            container.innerHTML = `
                <div style="margin-bottom: 10px;">
                    <strong>Total Records:</strong> ${allRecords.length.toLocaleString()} | 
                    <strong>Showing:</strong> ${start + 1} - ${end}
                    <span style="margin-left: 15px; color: #666; font-style: italic;">Click any row to view details | Click column headers to sort</span>
                </div>
                <div style="overflow-x: auto;">
                    <table id="recordsTable">
                        <thead>
                            <tr>${columns.map(col => `<th onclick="sortTable('${col}')" data-column="${col}">${col}</th>`).join('')}</tr>
                        </thead>
                        <tbody>
                            ${pageData.map((row, idx) => `
                                <tr onclick="showRecordDetail(${start + idx})">
                                    ${columns.map(col => `<td>${row[col] !== null && row[col] !== undefined ? row[col] : ''}</td>`).join('')}
                                </tr>
                            `).join('')}
                        </tbody>
                    </table>
                </div>
                <div class="pagination">
                    <button ${currentPage === 0 ? 'disabled' : ''} onclick="loadRecords(${currentPage - 1})">Previous</button>
                    <span>Page ${currentPage + 1} of ${totalPages}</span>
                    <button ${currentPage >= totalPages - 1 ? 'disabled' : ''} onclick="loadRecords(${currentPage + 1})">Next</button>
                </div>
            `;
            
            // Restore sort indicator if column was sorted
            if (sortColumn) {
                const th = document.querySelector(`th[data-column="${sortColumn}"]`);
                if (th) {
                    th.classList.add(sortDirection === 'asc' ? 'sort-asc' : 'sort-desc');
                }
            }
        }
        
        function sortTable(column) {
            // Toggle sort direction if same column, otherwise default to ascending
            if (sortColumn === column) {
                sortDirection = sortDirection === 'asc' ? 'desc' : 'asc';
            } else {
                sortColumn = column;
                sortDirection = 'asc';
            }
            
            // Remove all sort classes
            document.querySelectorAll('th').forEach(th => {
                th.classList.remove('sort-asc', 'sort-desc');
            });
            
            // Sort ALL records in the dataset
            allRecords.sort((a, b) => {
                let valA = a[column];
                let valB = b[column];
                
                // Handle null/undefined
                if (valA === null || valA === undefined) return 1;
                if (valB === null || valB === undefined) return -1;
                
                // Convert to string for comparison
                valA = String(valA).toLowerCase();
                valB = String(valB).toLowerCase();
                
                // Check if numeric
                const numA = parseFloat(valA);
                const numB = parseFloat(valB);
                
                if (!isNaN(numA) && !isNaN(numB)) {
                    return sortDirection === 'asc' ? numA - numB : numB - numA;
                }
                
                // String comparison
                if (valA < valB) return sortDirection === 'asc' ? -1 : 1;
                if (valA > valB) return sortDirection === 'asc' ? 1 : -1;
                return 0;
            });
            
            // Reset to first page after sorting
            currentPage = 0;
            
            // Redisplay with sorted data
            displayRecords();
        }
        
        function showRecordDetail(index) {
            const record = allRecords[index];
            const modal = document.getElementById('recordModal');
            const modalBody = document.getElementById('modalBody');
            const modalTitle = document.getElementById('modalTitle');
            
            // Try to find a good identifier for the title
            const titleField = record.Name || record.name || record.TDCJ || record.id || record.ID || `Record ${index + 1}`;
            modalTitle.textContent = titleField;
            
            // Build the modal content
            const fields = Object.keys(record).map(key => `
                <div class="record-field">
                    <div class="record-field-label">${key}</div>
                    <div class="record-field-value">${record[key] !== null && record[key] !== undefined ? record[key] : '<em style="color: #999;">N/A</em>'}</div>
                </div>
            `).join('');
            
            modalBody.innerHTML = fields;
            modal.style.display = 'block';
        }
        
        function closeModal() {
            document.getElementById('recordModal').style.display = 'none';
        }
        
        // Close modal when clicking outside of it
        window.onclick = function(event) {
            const modal = document.getElementById('recordModal');
            if (event.target === modal) {
                closeModal();
            }
        }
        
        // Close modal with Escape key
        document.addEventListener('keydown', function(event) {
            if (event.key === 'Escape') {
                closeModal();
            }
        });
        
        // Initialize
        loadPickleFiles();
        loadParquetFiles();
    </script>
</body>
</html>