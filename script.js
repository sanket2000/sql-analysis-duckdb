// Initialize and retrieve the DuckDB instance
const getDb = async () => {
    const duckdb = window.duckdbWasm;
    if (window._db) return window._db; // Reuse existing instance

    try {
        const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();
        const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);

        // Create a worker from the selected bundle
        const worker_url = URL.createObjectURL(
            new Blob([`importScripts("${bundle.mainWorker}");`], {
                type: "text/javascript",
            })
        );
        const worker = new Worker(worker_url);

        // Initialize DuckDB with a logger
        const logger = new duckdb.ConsoleLogger();
        const db = new duckdb.AsyncDuckDB(logger, worker);

        // Instantiate the database
        await db.instantiate(bundle.mainModule, bundle.pthreadWorker);

        // Clean up the worker URL and store the database instance
        URL.revokeObjectURL(worker_url);
        window._db = db;
        return db;
    } catch (error) {
        console.error("Error initializing DuckDB:", error);
        alert("Failed to initialize DuckDB. Check the console for details.");
    }
};

// Function to handle CSV file reading
const readCSVFile = (file) => {
    return new Promise((resolve, reject) => {
        const reader = new FileReader();
        reader.onload = (e) => resolve(e.target.result);
        reader.onerror = (e) => reject(e);
        reader.readAsText(file);
    });
};

function convertToCSV(data) {
    // Get the headers
    const headers = Object.keys(data[0]);

    // Map data to CSV format
    const rows = data.map(row => headers.map(field => JSON.stringify(row[field] || '')).join(','));

    // Combine headers and rows
    return [headers.join(','), ...rows].join('\n');
}

// Function to run the SQL query on the uploaded CSV
const runQuery = async () => {
    const db = await getDb();
    if (!db) throw new Error("DuckDB initialization failed.");
    const conn = await db.connect();

    // Check if a CSV file is uploaded
    const fileInput = document.getElementById('csvFile').files[0];
    if (!fileInput) return alert("Please upload a CSV file.");
    console.log(fileInput);

    // Read CSV data
    const csvData = await readCSVFile(fileInput);

    // Load CSV data into DuckDB table
    await db.registerFileText(fileInput.name, csvData);
    result = await conn.query(`CREATE OR REPLACE TABLE my_table AS FROM '${fileInput.name}';`);

    // Execute user-provided SQL query
    const sql = document.getElementById('sqlQuery').value;
    result = await conn.query(sql);
    _result = convertToCSV(JSON.parse(result.toString()))
    document.getElementById('sqlResult').value = result;

    // Convert result to CSV for download
    const csvContent = "data:text/csv;charset=utf-8," + _result;
    const downloadLink = document.getElementById('downloadLink');
    downloadLink.setAttribute("href", URL.createObjectURL(new Blob([csvContent])));
    downloadLink.setAttribute("download", "query_result.csv");
    downloadLink.style.display = "block";
};