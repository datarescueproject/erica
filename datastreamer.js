/**
 * Data Streamer - A utility for streaming and processing structured data files
 */
class DataStreamer {
  constructor(options = {}) {
    this.options = {
      firstChunkMinSize: options.firstChunkMinSize || 100,
      progressiveChunkSize: options.progressiveChunkSize || 500,
      ...options
    };
    
    this.data = {
      firstChunkRows: [],
      progressiveChunks: [],
      isFirstChunkLoaded: false,
      totalRowCount: 0
    };
    
    this.loadStartTime = null;
    this.currentBuffer = [];
  }

  /**
   * Starts streaming a JSONL file
   * @param {string} url - URL to the JSONL file (compressed or uncompressed)
   * @param {Function} onFirstChunkLoaded - Callback when first chunk is loaded
   * @param {Function} onProgressiveChunkLoaded - Callback when a progressive chunk is loaded
   * @param {Function} onAllDataLoaded - Callback when all data is loaded
   * @param {Function} onError - Callback when an error occurs
   * @param {Function} onLoadingStarted - Callback when loading starts
   * @returns {Promise} Promise that resolves when streaming is complete
   */
  async streamJSONL(url, onFirstChunkLoaded, onProgressiveChunkLoaded, onAllDataLoaded, onError, onLoadingStarted) {
    this.loadStartTime = +(new Date());
    this.data = {
      firstChunkRows: [],
      progressiveChunks: [],
      isFirstChunkLoaded: false,
      totalRowCount: 0
    };
    
    this.currentBuffer = [];
    
    // Notify that loading has started
    if (onLoadingStarted) {
      onLoadingStarted();
    }
    
    try {
      await this._streamAndProcess(url, onFirstChunkLoaded, onProgressiveChunkLoaded, onAllDataLoaded);
      return {
        totalRowCount: this.data.totalRowCount,
        loadTime: (+(new Date()) - this.loadStartTime) / 1000
      };
    } catch (error) {
      console.error('Error loading JSONL file:', error);
      if (onError) {
        onError(error);
      }
      throw error;
    }
  }

  /**
   * Returns all loaded data
   * @returns {Array} All rows loaded from the JSONL file
   */
  getAllData() {
    return [
      ...this.data.firstChunkRows, 
      ...this.data.progressiveChunks.flat()
    ];
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
  async _streamAndProcess(url, onFirstChunkLoaded, onProgressiveChunkLoaded, onAllDataLoaded) {
    // Fetch the file, using cache if available
    const response = await fetch(url, {
      cache: 'force-cache' // Use cached version if available
    });
    
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    
    // Check if the response came from cache
    const fromCache = response.headers.get('x-from-cache') === 'true' || 
                     (response.headers.get('age') !== null) ||
                     (response.headers.get('cf-cache-status') === 'HIT');
    
    if (fromCache) {
      // If cached, process the entire file at once
      console.log('Using cached file - processing entire file at once');
      const text = await response.text();
      const allRows = this._parseJSONLBuffer(text);
      
      // Handle first chunk notification
      this.data.firstChunkRows = allRows.slice(0, this.options.firstChunkMinSize);
      this.data.isFirstChunkLoaded = true;
      this.data.totalRowCount = allRows.length;
      
      if (onFirstChunkLoaded) {
        const firstChunkTime = +(new Date());
        const timeTaken = (firstChunkTime - this.loadStartTime) / 1000;
        
        onFirstChunkLoaded({
          rows: this.data.firstChunkRows,
          count: this.data.firstChunkRows.length,
          time: timeTaken
        });
      }
      
      // Store remaining rows as a single progressive chunk
      const remainingRows = allRows.slice(this.options.firstChunkMinSize);
      if (remainingRows.length > 0) {
        this.data.progressiveChunks.push(remainingRows);
        
        if (onProgressiveChunkLoaded) {
          const currentTime = +(new Date());
          const timeTaken = (currentTime - this.loadStartTime) / 1000;
          
          onProgressiveChunkLoaded({
            rows: remainingRows,
            count: remainingRows.length,
            totalCount: this.data.totalRowCount,
            time: timeTaken
          });
        }
      }
      
      // Finalize loading
      this._finalizeLoading(onFirstChunkLoaded, onAllDataLoaded);
    } else {
      // If not cached, use streaming for progressive loading
      console.log('File not cached - using streaming approach');
      
      // Get a reader for the stream
      const reader = response.body.getReader();
      
      let decoder = new TextDecoder();
      let buffer = '';
      
      while (true) {
        const { done, value } = await reader.read();
        
        if (done) {
          if (buffer) {
            this._processBuffer(buffer, onFirstChunkLoaded, onProgressiveChunkLoaded);
          }
          
          // Send any remaining buffered rows as a final chunk
          if (this.currentBuffer.length > 0) {
            this._sendProgressiveChunk(onProgressiveChunkLoaded);
          }
          
          this._finalizeLoading(onFirstChunkLoaded, onAllDataLoaded);
          break;
        }
        
        // Convert the chunk to text and add to our buffer
        buffer += decoder.decode(value, { stream: true });
        
        // Process complete lines from the buffer
        const lines = buffer.split('\n');
        buffer = lines.pop() || ''; // Keep the last incomplete line in the buffer
        
        if (lines.length > 0) {
          this._processBuffer(lines, onFirstChunkLoaded, onProgressiveChunkLoaded);
        }
      }
    }
  }

  /**
   * Process an array of lines into JSON objects
   * @private
   */
  _processBuffer(lines, onFirstChunkLoaded, onProgressiveChunkLoaded) {
    // Parse each complete line as JSON
    const rows = lines
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
    
    if (rows.length === 0) return;
    
    this.data.totalRowCount += rows.length;
    
    // Handle based on our first chunk status
    if (!this.data.isFirstChunkLoaded) {
      this.data.firstChunkRows = this.data.firstChunkRows.concat(rows);
      
      // If we have enough rows for the first chunk, notify
      if (this.data.firstChunkRows.length >= this.options.firstChunkMinSize) {
        this.data.isFirstChunkLoaded = true;
        
        if (onFirstChunkLoaded) {
          const firstChunkTime = +(new Date());
          const timeTaken = (firstChunkTime - this.loadStartTime) / 1000;
          
          onFirstChunkLoaded({
            rows: this.data.firstChunkRows,
            count: this.data.firstChunkRows.length,
            time: timeTaken
          });
          
          // Set initial update time after first chunk
          this.lastUpdateTime = firstChunkTime;
        }
      }
    } else {
      // After first chunk, add to current buffer for progressive updates
      this.currentBuffer = this.currentBuffer.concat(rows);
      
      // If we've accumulated enough rows for a progressive update, send them
      if (this.currentBuffer.length >= this.options.progressiveChunkSize) {
        this._sendProgressiveChunk(onProgressiveChunkLoaded);
      }
    }
  }
  
  /**
   * Send a progressive chunk update
   * @private
   */
  _sendProgressiveChunk(onProgressiveChunkLoaded) {
    if (this.currentBuffer.length > 0 && onProgressiveChunkLoaded) {
      const currentTime = +(new Date());
      const timeTaken = (currentTime - this.loadStartTime) / 1000;
      
      // Store this chunk in our data structure
      this.data.progressiveChunks.push(this.currentBuffer);
      
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
  }

  /**
   * Finalize loading by handling small file edge cases
   * @private
   */
  _finalizeLoading(onFirstChunkLoaded, onAllDataLoaded) {
    // If first chunk wasn't loaded yet (small file case)
    if (!this.data.isFirstChunkLoaded && this.data.firstChunkRows.length > 0) {
      this.data.isFirstChunkLoaded = true;
      
      if (onFirstChunkLoaded) {
        const firstChunkTime = +(new Date());
        const timeTaken = (firstChunkTime - this.loadStartTime) / 1000;
        
        onFirstChunkLoaded({
          rows: this.data.firstChunkRows,
          count: this.data.firstChunkRows.length,
          time: timeTaken
        });
      }
    }
    
    // Force sending any remaining buffered data
    if (this.currentBuffer.length > 0 && this.data.isFirstChunkLoaded) {
      this.data.progressiveChunks.push(this.currentBuffer);
    }
    
    // Notify that all data is now loaded
    if (onAllDataLoaded) {
      const fullLoadTime = +(new Date());
      const timeTaken = (fullLoadTime - this.loadStartTime) / 1000;
      
      onAllDataLoaded({
        rows: this.getAllData(),
        count: this.data.totalRowCount,
        time: timeTaken
      });
    }
  }
  
  /**
   * Parse JSONL buffer into array of objects
   * @private
   */
  _parseJSONLBuffer(buffer) {
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
}