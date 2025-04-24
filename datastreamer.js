/**
 * Data Streamer - A utility for streaming and processing structured data files
 */
class DataStreamer {
  constructor(options = {}) {
    this.options = {
      firstChunkMinSize: options.firstChunkMinSize || 100,
      progressiveChunkSize: options.progressiveChunkSize || 500,
      preCheckCache: options.preCheckCache !== undefined ? options.preCheckCache : true,
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
    this.cacheStatus = null;
  }
  
  /**
   * Detect if a resource is already cached
   * @param {string} url - URL to check
   * @returns {Promise<boolean>} Whether the resource is cached
   */
  async detectCacheStatus(url) {
    if (!this.options.preCheckCache) return false;
    
    try {
      console.log('Checking if resource is cached...');
      const headResponse = await fetch(url, {
        method: 'HEAD',
        cache: 'force-cache'
      });
      
      const fromCache = headResponse.headers.get('x-from-cache') === 'true' || 
                      (headResponse.headers.get('age') !== null) ||
                      (headResponse.headers.get('cf-cache-status') === 'HIT');
                      
      console.log(`Cache pre-check result: ${fromCache ? 'CACHED' : 'NOT CACHED'}`);
      return fromCache;
    } catch (e) {
      console.warn('Cache detection failed:', e);
      return false;
    }
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
    
    // Reset cache status for this new request
    this.cacheStatus = null;
    
    // Check if the resource is cached before starting the actual load process
    if (this.options.preCheckCache) {
      this.cacheStatus = await this.detectCacheStatus(url);
    }
    
    // Notify that loading has started with cache status information
    if (onLoadingStarted) {
      onLoadingStarted(this.cacheStatus);
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
   * Core streaming and processing implementation using a unified buffer approach
   * @private
   */
  async _streamAndProcess(url, onFirstChunkLoaded, onProgressiveChunkLoaded, onAllDataLoaded) {
    // Use our cache detection if it hasn't been done already
    let fromCache = this.cacheStatus;
    
    if (fromCache === null && this.options.preCheckCache) {
      fromCache = await this.detectCacheStatus(url);
      this.cacheStatus = fromCache;
    }
    
    // Fetch the file, using cache if available
    const response = await fetch(url, {
      cache: 'force-cache' // Use cached version if available
    });
    
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    
    // Double-check cache status from the actual response if not already determined
    if (fromCache === null || fromCache === false) {
      const responseFromCache = response.headers.get('x-from-cache') === 'true' || 
                              (response.headers.get('age') !== null) ||
                              (response.headers.get('cf-cache-status') === 'HIT');
      
      if (responseFromCache) {
        console.log('Cache status detected from response headers');
        fromCache = true;
        this.cacheStatus = true;
      }
    }
    
    console.log(`Processing file with unified buffer approach (fromCache: ${fromCache ? 'yes' : 'no'})`);
    
    // Initialize stream reader and decoder
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    
    // Step 1: Stream first chunk for both cached and non-cached files
    let buffer = '';
    let firstChunkComplete = false;
    
    // Stream until we have enough for the first chunk
    while (!firstChunkComplete) {
      const { done, value } = await reader.read();
      
      if (done) {
        // File is smaller than first chunk size or exactly that size
        if (buffer) {
          this._processFirstChunkFromBuffer(buffer, onFirstChunkLoaded);
        }
        
        // Finalize loading since we've reached the end of the file
        this._finalizeLoading(onFirstChunkLoaded, onAllDataLoaded);
        return;
      }
      
      // Add new data to buffer
      buffer += decoder.decode(value, { stream: true });
      
      // Process complete lines from the buffer
      const lines = buffer.split('\n');
      buffer = lines.pop() || ''; // Keep incomplete line in buffer
      
      // Process lines
      const rows = this._parseJSONLLines(lines);
      
      if (rows.length > 0) {
        this.data.totalRowCount += rows.length;
        this.data.firstChunkRows = this.data.firstChunkRows.concat(rows);
        
        // If we have enough for first chunk, report it and move on
        if (!this.data.isFirstChunkLoaded && this.data.firstChunkRows.length >= this.options.firstChunkMinSize) {
          this.data.isFirstChunkLoaded = true;
          
          if (onFirstChunkLoaded) {
            const firstChunkTime = +(new Date());
            const timeTaken = (firstChunkTime - this.loadStartTime) / 1000;
            
            onFirstChunkLoaded({
              rows: this.data.firstChunkRows.slice(0, this.options.firstChunkMinSize),
              count: this.options.firstChunkMinSize,
              time: timeTaken
            });
            
            // Keep excess rows for the next chunk
            this.currentBuffer = this.data.firstChunkRows.slice(this.options.firstChunkMinSize);
            
            firstChunkComplete = true;
            break;
          }
        }
      }
    }
    
    // Step 2: Handle the rest of the file based on cache status
    if (fromCache) {
      // For cached files: load the rest of the file at once
      console.log('Loading rest of cached file at once');
      
      // Read the remaining data in one operation
      const chunks = [];
      
      while (true) {
        const { done, value } = await reader.read();
        
        if (done) break;
        
        chunks.push(value);
      }
      
      // Combine chunks and process
      const remainingData = buffer + decoder.decode(new Uint8Array(
        chunks.reduce((acc, chunk) => {
          const tmp = new Uint8Array(acc.length + chunk.length);
          tmp.set(acc, 0);
          tmp.set(chunk, acc.length);
          return tmp;
        }, new Uint8Array(0))
      ));
      
      // Process the entire remaining file
      const lines = remainingData.split('\n');
      const rows = this._parseJSONLLines(lines.filter(line => line.trim() !== ''));
      
      if (rows.length > 0) {
        this.data.totalRowCount += rows.length;
        
        // Send as a single large progressive chunk
        if (onProgressiveChunkLoaded) {
          const currentTime = +(new Date());
          const timeTaken = (currentTime - this.loadStartTime) / 1000;
          
          onProgressiveChunkLoaded({
            rows: [...this.currentBuffer, ...rows],
            count: this.currentBuffer.length + rows.length,
            totalCount: this.data.totalRowCount,
            time: timeTaken
          });
        }
      }
      
      // Finalize loading
      this._finalizeLoading(onFirstChunkLoaded, onAllDataLoaded);
      
    } else {
      // For non-cached files: continue streaming in progressive chunks
      console.log('Streaming remaining data in progressive chunks');
      
      while (true) {
        const { done, value } = await reader.read();
        
        if (done) {
          // Process any remaining data in the buffer
          if (buffer && buffer.trim() !== '') {
            const rows = this._parseJSONLBuffer(buffer);
            
            if (rows.length > 0) {
              this.data.totalRowCount += rows.length;
              this.currentBuffer = this.currentBuffer.concat(rows);
            }
          }
          
          // Send any remaining buffered rows as a final chunk
          if (this.currentBuffer.length > 0) {
            if (onProgressiveChunkLoaded) {
              const currentTime = +(new Date());
              const timeTaken = (currentTime - this.loadStartTime) / 1000;
              
              onProgressiveChunkLoaded({
                rows: this.currentBuffer,
                count: this.currentBuffer.length,
                totalCount: this.data.totalRowCount,
                time: timeTaken
              });
              
              this.currentBuffer = [];
            }
          }
          
          // Finalize loading
          this._finalizeLoading(onFirstChunkLoaded, onAllDataLoaded);
          break;
        }
        
        // Add new data to buffer
        buffer += decoder.decode(value, { stream: true });
        
        // Process complete lines from the buffer
        const lines = buffer.split('\n');
        buffer = lines.pop() || ''; // Keep incomplete line in buffer
        
        // Process lines
        const rows = this._parseJSONLLines(lines);
        
        if (rows.length > 0) {
          this.data.totalRowCount += rows.length;
          this.currentBuffer = this.currentBuffer.concat(rows);
          
          // If we've accumulated enough for a progressive update, send it
          if (this.currentBuffer.length >= this.options.progressiveChunkSize) {
            if (onProgressiveChunkLoaded) {
              const currentTime = +(new Date());
              const timeTaken = (currentTime - this.loadStartTime) / 1000;
              
              onProgressiveChunkLoaded({
                rows: this.currentBuffer,
                count: this.currentBuffer.length, 
                totalCount: this.data.totalRowCount,
                time: timeTaken
              });
              
              // Reset buffer after sending
              this.currentBuffer = [];
            }
          }
        }
      }
    }
  }
  
  /**
   * Process first chunk from a buffer
   * @private
   */
  _processFirstChunkFromBuffer(buffer, onFirstChunkLoaded) {
    if (!buffer || buffer.trim() === '') return;
    
    const rows = this._parseJSONLBuffer(buffer);
    
    if (rows.length === 0) return;
    
    this.data.totalRowCount += rows.length;
    this.data.firstChunkRows = rows;
    this.data.isFirstChunkLoaded = true;
    
    if (onFirstChunkLoaded) {
      const firstChunkTime = +(new Date());
      const timeTaken = (firstChunkTime - this.loadStartTime) / 1000;
      
      onFirstChunkLoaded({
        rows: rows,
        count: rows.length,
        time: timeTaken
      });
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
  
  /**
   * Parse array of JSONL lines into array of objects
   * @private
   */
  _parseJSONLLines(lines) {
    return lines
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