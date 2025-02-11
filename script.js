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

const showError = (message) => {
    document.getElementById('errorMessage').innerText = message;
};

// Function to run the SQL query on the uploaded CSV
const runQuery = async () => {
    try {
        document.getElementById('errorMessage').innerText = "";
        document.getElementById('sqlResult').value = "";
        const db = await getDb();
        if (!db) throw new Error("DuckDB initialization failed.");
        const conn = await db.connect();

        const fileInput = document.getElementById('csvFile').files;
        if (!fileInput.length) return alert("Please upload at least one CSV file.");

        // Process each CSV file
        for (const file of fileInput) {
            const csvData = await readCSVFile(file);
            await db.registerFileText(file.name, csvData);
            await conn.query(`CREATE OR REPLACE TABLE ${file.name.split('.')[0]} AS FROM '${file.name}';`);
        }

        // Execute user-provided SQL query
        const sql = document.getElementById('sqlQuery').value;
        result = await conn.query(sql);
        document.getElementById('sqlResult').value = result.toString();

        // Convert result to CSV for download
        const csvContent = convertToCSV(JSON.parse(result.toString()))
        const downloadLink = document.getElementById('downloadLink');

        // Create a blob with a MIME type of text/csv
        const blob = new Blob([csvContent], { type: 'text/csv' });
        downloadLink.setAttribute("href", URL.createObjectURL(blob));
        downloadLink.setAttribute("download", "query_result.csv");
        downloadLink.style.display = "block";
    } catch (error) {
        showError("Error: " + error.message);
        console.error(error);
    }
};