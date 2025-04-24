/**
 * DataStreamer - A utility for streaming and processing structured data files
 */
class DataStreamer {
  constructor(options = {}) {
    this.options = {
      firstChunkMinSize: options.firstChunkMinSize || 100,
      progressiveChunkSize: options.progressiveChunkSize || 500,
      ...options
    };
    
    this.reset();
  }

  /**
   * Resets internal state
   * @private
   */
  reset() {
    this.data = {
      firstChunkRows: [],
      progressiveChunks: [],
      isFirstChunkLoaded: false,
      totalRowCount: 0
    };
    
    this.loadStartTime = null;
    this.currentBuffer = [];
    this.currentRowCount = 0;
  }
  
  /**
   * Starts streaming a JSONL file
   * @param {string} url - URL to the JSONL file
   * @param {Object} callbacks - Callback functions
   * @returns {Promise} Promise that resolves when streaming is complete
   */
  async streamJSONL(url, {
    onFirstChunkLoaded = null,
    onProgressiveChunkLoaded = null,
    onAllDataLoaded = null,
    onError = null,
    onLoadingStarted = null
  } = {}) {
    // Support for legacy callback style
    if (typeof arguments[1] === 'function') {
      onFirstChunkLoaded = arguments[1];
      onProgressiveChunkLoaded = arguments[2];
      onAllDataLoaded = arguments[3];
      onError = arguments[4];
      onLoadingStarted = arguments[5];
    }
    
    this.reset();
    this.loadStartTime = Date.now();
    
    if (onLoadingStarted) onLoadingStarted();
    
    try {
      // Check for HEAD support to get content size if available
      let contentLength;
      try {
        const headResponse = await fetch(url, { method: 'HEAD' });
        contentLength = headResponse.headers.get('Content-Length');
      } catch (e) {
        // Ignore HEAD errors, we'll proceed without content length
      }
      
      await this._streamAndProcess(url, {
        onFirstChunkLoaded,
        onProgressiveChunkLoaded,
        onAllDataLoaded
      }, contentLength);
      
      return {
        totalRowCount: this.data.totalRowCount,
        loadTime: (Date.now() - this.loadStartTime) / 1000
      };
    } catch (error) {
      console.error('Error loading JSONL file:', error);
      if (onError) onError(error);
      throw error;
    }
  }

  /**
   * Returns all loaded data
   * @returns {Array} All rows loaded from the JSONL file
   */
  getAllData() {
    // Using faster array concat for large datasets
    const allData = new Array(this.data.totalRowCount);
    
    let position = 0;
    // Copy first chunk
    for (let i = 0; i < this.data.firstChunkRows.length; i++) {
      allData[position++] = this.data.firstChunkRows[i];
    }
    
    // Copy progressive chunks
    for (const chunk of this.data.progressiveChunks) {
      for (let i = 0; i < chunk.length; i++) {
        allData[position++] = chunk[i];
      }
    }
    
    return allData;
  }
  
  /**
   * Returns first chunk data
   * @returns {Array} First chunk of data
   */
  getFirstChunkData() {
    return this.data.firstChunkRows;
  }

  /**
   * Core streaming and processing implementation
   * @private
   */
  async _streamAndProcess(url, { onFirstChunkLoaded, onProgressiveChunkLoaded, onAllDataLoaded }, contentLength) {
    const response = await fetch(url, { cache: 'force-cache' });
    
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    let firstChunkComplete = false;
    
    // Pre-allocate array for better performance
    const preAllocSize = 10000;
    let rowBuffer = new Array(preAllocSize);
    let rowCount = 0;
    this.currentRowCount = 0;
    
    // Process stream until complete
    let bytesReceived = 0;
    
    while (true) {
      const { done, value } = await reader.read();
      
      if (done) {
        // Firefox requires an explicit call to decode with stream:false to flush the decoder
        buffer += decoder.decode(new Uint8Array(0), { stream: false });
        
        // Force process any remaining buffer content
        if (buffer.trim()) {
          const remainingLines = buffer.split('\n');
          this._processLines(remainingLines, rowBuffer, rowCount, firstChunkComplete, 
            onFirstChunkLoaded, onProgressiveChunkLoaded);
        }
        
        // Send any final buffered rows
        if (this.currentBuffer.length > 0) {
          this._sendProgressiveChunk(onProgressiveChunkLoaded);
        }
        
        this._finalizeLoading({ onFirstChunkLoaded, onProgressiveChunkLoaded, onAllDataLoaded });
        break;
      }
      
      bytesReceived += value.length;
      buffer += decoder.decode(value, { stream: true });
      
      // Process complete lines
      const newlineIndex = buffer.lastIndexOf('\n');
      
      // If we have complete lines, process them
      if (newlineIndex !== -1) {
        const completeLines = buffer.substring(0, newlineIndex).split('\n');
        buffer = buffer.substring(newlineIndex + 1);
        
        this._processLines(completeLines, rowBuffer, rowCount, firstChunkComplete, 
          onFirstChunkLoaded, onProgressiveChunkLoaded);
        
        // Update tracking variables for next iteration
        rowCount = this.currentRowCount || 0;
        firstChunkComplete = this.data.isFirstChunkLoaded;
      } else if (buffer.length > 100000) {
        // If buffer is getting too large but no newlines, try to process anyway
        console.warn('Processing large buffer without newlines, forcing split');
        const lines = buffer.split('\n');
        
        // Keep the last (potentially incomplete) line in the buffer
        buffer = lines.pop() || '';
        
        if (lines.length > 0) {
          this._processLines(lines, rowBuffer, rowCount, firstChunkComplete,
            onFirstChunkLoaded, onProgressiveChunkLoaded);
          
          // Update tracking variables
          rowCount = this.currentRowCount || 0;
          firstChunkComplete = this.data.isFirstChunkLoaded;
        }
      }
      
      // Process progressive chunks if we have enough data
      if (this.currentBuffer.length >= this.options.progressiveChunkSize) {
        this._sendProgressiveChunk(onProgressiveChunkLoaded);
      }
    }
  }
  
  /**
   * Process a batch of lines from the buffer
   * @private
   */
  _processLines(lines, rowBuffer, rowCount, firstChunkComplete, onFirstChunkLoaded, onProgressiveChunkLoaded) {
    if (!lines || lines.length === 0) return;
    
    // Parse JSON lines
    for (let i = 0; i < lines.length; i++) {
      const line = lines[i];
      if (!line.trim()) continue;
      
      try {
        const parsedLine = JSON.parse(line);
        
        // Add to row buffer
        if (rowCount < rowBuffer.length) {
          rowBuffer[rowCount++] = parsedLine;
        } else {
          // Process batch if buffer is full
          this._processBatch(rowBuffer, rowCount, firstChunkComplete, 
            onFirstChunkLoaded, onProgressiveChunkLoaded);
          
          // Create new buffer with double capacity
          const newSize = rowBuffer.length * 2;
          rowBuffer = new Array(newSize);
          rowBuffer[0] = parsedLine;
          rowCount = 1;
          
          // Update firstChunkComplete status
          firstChunkComplete = this.data.isFirstChunkLoaded;
        }
      } catch (e) {
        console.error('Error parsing JSON line:', line);
      }
    }
    
    // Process any remaining rows
    if (rowCount > 0) {
      this._processBatch(rowBuffer, rowCount, firstChunkComplete, 
        onFirstChunkLoaded, onProgressiveChunkLoaded);
      
      // Store current state for next iteration
      this.currentRowCount = 0;
    }
  }
  
  /**
   * Process a batch of rows
   * @private
   */
  _processBatch(rowBuffer, rowCount, firstChunkComplete, onFirstChunkLoaded, onProgressiveChunkLoaded) {
    if (rowCount === 0) return;
    
    this.data.totalRowCount += rowCount;
    
    // Handle first chunk if not yet processed
    if (!firstChunkComplete && !this.data.isFirstChunkLoaded) {
      const rowsNeeded = this.options.firstChunkMinSize;
      
      // Check if we have enough for first chunk
      if (rowCount >= rowsNeeded) {
        // Slice array to get first chunk
        const firstChunkRows = rowBuffer.slice(0, rowsNeeded);
        this.data.firstChunkRows = firstChunkRows;
        this.data.isFirstChunkLoaded = true;
        
        if (onFirstChunkLoaded) {
          const timeTaken = (Date.now() - this.loadStartTime) / 1000;
          onFirstChunkLoaded({
            rows: firstChunkRows,
            count: rowsNeeded,
            time: timeTaken
          });
        }
        
        // Any remaining rows go to the progressive buffer
        if (rowCount > rowsNeeded) {
          this.currentBuffer = this.currentBuffer.concat(
            rowBuffer.slice(rowsNeeded, rowCount)
          );
        }
      } else {
        // Not enough for first chunk yet, store what we have
        this.data.firstChunkRows = rowBuffer.slice(0, rowCount);
      }
    } else {
      // Already past first chunk, add to progressive buffer
      this.currentBuffer = this.currentBuffer.concat(rowBuffer.slice(0, rowCount));
    }
  }

  /**
   * Send a progressive chunk update
   * @private
   */
  _sendProgressiveChunk(onProgressiveChunkLoaded) {
    if (this.currentBuffer.length === 0 || !onProgressiveChunkLoaded) return;
    
    const timeTaken = (Date.now() - this.loadStartTime) / 1000;
    
    // Store this chunk in our data structure
    this.data.progressiveChunks.push([...this.currentBuffer]);
    
    // Notify callback
    onProgressiveChunkLoaded({
      rows: this.currentBuffer,
      count: this.currentBuffer.length,
      totalCount: this.data.totalRowCount,
      time: timeTaken
    });
    
    // Reset buffer
    this.currentBuffer = [];
  }

  /**
   * Finalize loading and trigger callbacks
   * @private
   */
  _finalizeLoading({ onFirstChunkLoaded, onProgressiveChunkLoaded, onAllDataLoaded }) {
    // If first chunk wasn't loaded yet (small file case)
    if (!this.data.isFirstChunkLoaded && this.data.firstChunkRows.length > 0) {
      this.data.isFirstChunkLoaded = true;
      
      if (onFirstChunkLoaded) {
        const timeTaken = (Date.now() - this.loadStartTime) / 1000;
        onFirstChunkLoaded({
          rows: this.data.firstChunkRows,
          count: this.data.firstChunkRows.length,
          time: timeTaken
        });
      }
    }
    
    // Double check that we've processed all data
    if (this.currentBuffer.length > 0) {
      if (this.data.isFirstChunkLoaded && onProgressiveChunkLoaded) {
        this._sendProgressiveChunk(onProgressiveChunkLoaded);
      } else {
        this.data.progressiveChunks.push([...this.currentBuffer]);
        this.currentBuffer = [];
      }
    }
    
    // Notify that all data is now loaded
    if (onAllDataLoaded) {
      const timeTaken = (Date.now() - this.loadStartTime) / 1000;
      onAllDataLoaded({
        rows: this.getAllData(),
        count: this.data.totalRowCount,
        time: timeTaken
      });
    }
  }
}