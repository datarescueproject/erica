let ts0 = +(new Date());
let dataTable;
let loadingComplete = false;

$(document).ready(function(){
  // Initialize DataTable with empty data first
  dataTable = $('#erica').DataTable({
    data: [], // Start empty, we'll load data via streaming
    initComplete: () => {
      let ts1 = +(new Date());
      console.log(`Initial table setup completed in ${(ts1-ts0)/1000}s`);
      
      // After initialization, start the streaming load process
      streamJSONL('data/fulltext-eric-records-lite.jsonl', 100); // Adjust chunk size as needed
    },
    order: {
      'id': 'asc'
    },
    columns: [
      {data:'id', name:'id', title:'id', render: data => {
        if(data.match(/^E[DJ][0-9]{6,7}$/)) {
          return `<a href="https://web.archive.org/web/2/https://files.eric.ed.gov/fulltext/${data}.pdf" target="_blank">${data}</a>`;
        } else {
          return data;
        }
      }},
      {data:'title', name:'title', title:'title'},
      {data:'author[</br></br>]', name:'author', title:'author'},
      {data:'publicationdateyear', name:'publicationdateyear', title:'year'}
    ],
    processing: true,
    deferRender: true, // Add this for better performance with large datasets
    language: {
      processing: "Loading data..."
    }
  });
});

// Function to load JSONL with first chunk immediate display, rest in background
async function streamJSONL(url, firstChunkSize) {
  try {
    const loadStartTime = +(new Date());
    
    // Show loading indicator for remaining data
    const loadingIndicator = $('<div class="loading-remaining" style="position:fixed; bottom:10px; right:10px; padding:8px; background:rgba(0,0,0,0.7); color:white; border-radius:4px;">Loading initial data...</div>');
    $('body').append(loadingIndicator);
    
    // Collections for rows
    let firstChunkRows = [];
    let remainingRows = [];
    let isFirstChunkLoaded = false;
    let totalRowCount = 0;
    
    // Fetch the file as a stream
    const response = await fetch(url);
    
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    
    // Get a reader for the stream
    const reader = response.body.getReader();
    let decoder = new TextDecoder();
    let buffer = '';
    
    while (true) {
      const { done, value } = await reader.read();
      
      if (done) {
        // Process any remaining data in the buffer
        if (buffer) {
          const rows = processJSONLBuffer(buffer);
          if (!isFirstChunkLoaded) {
            firstChunkRows = firstChunkRows.concat(rows);
          } else {
            remainingRows = remainingRows.concat(rows);
          }
        }
        
        // If first chunk wasn't loaded yet (small file case)
        if (!isFirstChunkLoaded && firstChunkRows.length > 0) {
          dataTable.rows.add(firstChunkRows).draw();
          const firstChunkTime = +(new Date());
          console.log(`First chunk of ${firstChunkRows.length} rows loaded in ${(firstChunkTime-loadStartTime)/1000}s`);
          isFirstChunkLoaded = true;
        }
        
        // Add all remaining rows at once
        if (remainingRows.length > 0) {
          // If we already loaded first chunk, clear the table first
          if (isFirstChunkLoaded) {
            dataTable.clear();
            // Add all data at once (first chunk + remaining)
            dataTable.rows.add([...firstChunkRows, ...remainingRows]).draw();
          } else {
            dataTable.rows.add(remainingRows).draw();
          }
          
          const fullLoadTime = +(new Date());
          console.log(`Full data loaded in ${(fullLoadTime-loadStartTime)/1000}s, total ${totalRowCount} records`);
        }
        
        // Remove loading indicator
        loadingIndicator.remove();
        loadingComplete = true;
        break;
      }
      
      // Convert the chunk to text and add to our buffer
      buffer += decoder.decode(value, { stream: true });
      
      // Process complete lines from the buffer
      const lines = buffer.split('\n');
      buffer = lines.pop() || ''; // Keep the last incomplete line in the buffer
      
      if (lines.length > 0) {
        // Parse each complete line as JSON
        const rows = lines
          .filter(line => line.trim() !== '')
          .map(line => JSON.parse(line));
        
        totalRowCount += rows.length;
        
        // Handle based on our first chunk status
        if (!isFirstChunkLoaded) {
          firstChunkRows = firstChunkRows.concat(rows);
          
          // If we have enough rows for the first chunk, display them
          if (firstChunkRows.length >= firstChunkSize) {
            dataTable.rows.add(firstChunkRows).draw();
            const firstChunkTime = +(new Date());
            console.log(`First chunk of ${firstChunkRows.length} rows loaded in ${(firstChunkTime-loadStartTime)/1000}s`);
            isFirstChunkLoaded = true;
            
            // Update loading message
            loadingIndicator.text(`Loading remaining data…`);
          }
        } else {
          // After first chunk, collect remaining rows without drawing
          remainingRows = remainingRows.concat(rows);
          
          // Periodically update the loading indicator
          if (totalRowCount % firstChunkSize === 0) {
            loadingIndicator.text(`Loading remaining data…`);
          }
        }
      }
      
      // Give UI time to breathe
      await new Promise(resolve => setTimeout(resolve, 0));
    }
    
  } catch (error) {
    console.error('Error loading JSONL file:', error);
    $('body').append(`<div class="error-message" style="color:red">Error loading data: ${error.message}</div>`);
  }
}

// Helper function to process JSONL buffer
function processJSONLBuffer(buffer) {
  return buffer.split('\n')
    .filter(line => line.trim() !== '')
    .map(line => {
      try {
        return JSON.parse(line);
      } catch (e) {
        console.error('Error parsing JSON line:', line);
        return null;
      }
    })
    .filter(item => item !== null);
}