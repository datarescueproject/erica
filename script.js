let ts0 = Date.now();
let dataTable;
let loadingComplete = false;

$(document).ready(function(){
  // Initialize DataTable with empty data first
  dataTable = $('#erica').DataTable({
    data: [], // Start empty, we'll load data via streaming
    initComplete: () => {
      let ts1 = Date.now();
      console.log(`Initial table setup completed in ${(ts1-ts0)/1000}s`);
      
      // After initialization, start the streaming load process
      const streamer = new DataStreamer({
        firstChunkMinSize: 100,       // Show first results quickly
        progressiveChunkSize: 50000   // Standard size for streaming chunks
      });
      
      streamer.streamJSONL(
        'data/fulltext-eric-records-lite.jsonl', 
        {
          onFirstChunkLoaded: ({rows, count, time}) => {
            console.log(`First chunk of ${count} rows loaded in ${time}s`);
            dataTable.rows.add(rows).draw();
          },
          onProgressiveChunkLoaded: ({rows, count, totalCount, time}) => {
            console.log(`Progressive chunk of ${count} rows loaded (total: ${totalCount}, ${time}s)`);
            dataTable.rows.add(rows).draw(false);
          },
          onAllDataLoaded: ({count, time}) => {
            console.log(`Full data loaded in ${time}s, total ${count} records`);
            dataTable.draw(false);
            loadingComplete = true;
            hideLoadingIndicator();
          },
          onError: (error) => {
            console.error('Error loading JSONL file:', error);
            showErrorMessage(error);
            hideLoadingIndicator();
          },
          onLoadingStarted: () => {
            showLoadingIndicator();
          }
        }
      );
    },
    order: {
      name:'id',
      dir:'asc'
    },
    columns: [
      {data:'id', name:'id', title:'id', width: '100px', render: data => {
        if(data.match(/^E[DJ][0-9]{6,7}$/)) {
          return `<a href="https://web.archive.org/web/2oe_/https://files.eric.ed.gov/fulltext/${data}.pdf" target="_blank">${data}</a>`;
        } else {
          return data;
        }
      }},
      {data:'title', name:'title', title:'title'},
      {data:'author[; ]', name:'author', title:'author'},
      {data:'publicationdateyear', name:'publicationdateyear', title:'year', width: '25px'}
    ],
    processing: true,
    deferRender: true, // Add this for better performance with large datasets
    language: {
      processing: "Loading data..."
    }
  });
});

/**
 * Shows the loading indicator in the UI
 */
function showLoadingIndicator() {
  document.getElementById('loading-indicator').hidden = false;
}

/**
 * Hides the loading indicator from the UI
 */
function hideLoadingIndicator() {
  document.getElementById('loading-indicator').hidden = true;
}

/**
 * Displays an error message to the user
 * @param {Error} error - The error that occurred
 */
function showErrorMessage(error) {
  $('body').append(`<div class="error-message" style="color:red">Error loading data: ${error.message}</div>`);
}