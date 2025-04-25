class DataStreamer {
  constructor(options = {}) {
    this.firstChunkMinSize = options.firstChunkMinSize || 100;
    this.progressiveChunkSize = options.progressiveChunkSize || 500;
    this.firstChunkLoaded = false;
    this.totalRowCount = 0;
    this.loadStartTime = null;
    this.buffer = "";
    this.currentChunk = [];
  }

  async streamJSONL(url, {
    onFirstChunkLoaded = null,
    onProgressiveChunkLoaded = null,
    onAllDataLoaded = null,
    onError = null,
    onLoadingStarted = null
  } = {}) {
    // Handle legacy callback style
    if (typeof arguments[1] === 'function') {
      onFirstChunkLoaded = arguments[1];
      onProgressiveChunkLoaded = arguments[2];
      onAllDataLoaded = arguments[3];
      onError = arguments[4];
      onLoadingStarted = arguments[5];
    }

    // Reset state and start timing
    this.firstChunkLoaded = false;
    this.totalRowCount = 0;
    this.loadStartTime = Date.now();
    this.buffer = "";
    this.currentChunk = [];
    this.allData = [];

    if (onLoadingStarted) onLoadingStarted();

    try {
      const response = await fetch(url);
      
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      
      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      
      while (true) {
        const { done, value } = await reader.read();
        
        if (done) {
          // Process any remaining data in buffer
          if (this.buffer.trim()) {
            this._processLines(this.buffer.split('\n'), onFirstChunkLoaded, onProgressiveChunkLoaded);
          }
          
          // Send any remaining rows in current chunk
          if (this.currentChunk.length > 0) {
            this._sendProgressiveChunk(onProgressiveChunkLoaded);
          }
          
          // Trigger completion callback
          if (onAllDataLoaded) {
            const timeTaken = (Date.now() - this.loadStartTime) / 1000;
            onAllDataLoaded({
              rows: this.allData,
              count: this.totalRowCount,
              time: timeTaken
            });
          }
          
          break;
        }
        
        // Decode chunk and add to buffer
        this.buffer += decoder.decode(value, { stream: true });
        
        // Process complete lines
        const newlineIndex = this.buffer.lastIndexOf('\n');
        if (newlineIndex !== -1) {
          const lines = this.buffer.substring(0, newlineIndex).split('\n');
          this.buffer = this.buffer.substring(newlineIndex + 1);
          this._processLines(lines, onFirstChunkLoaded, onProgressiveChunkLoaded);
        }
      }
      
      return {
        totalRowCount: this.totalRowCount,
        loadTime: (Date.now() - this.loadStartTime) / 1000
      };
      
    } catch (error) {
      console.error('Error loading JSONL file:', error);
      if (onError) onError(error);
      throw error;
    }
  }
  
  _processLines(lines, onFirstChunkLoaded, onProgressiveChunkLoaded) {
    for (const line of lines) {
      if (!line.trim()) continue;
      
      try {
        const parsedRow = JSON.parse(line);
        this.totalRowCount++;
        this.allData.push(parsedRow);
        
        // First chunk handling
        if (!this.firstChunkLoaded) {
          if (this.totalRowCount >= this.firstChunkMinSize) {
            this.firstChunkLoaded = true;
            
            if (onFirstChunkLoaded) {
              const timeTaken = (Date.now() - this.loadStartTime) / 1000;
              onFirstChunkLoaded({
                rows: this.allData.slice(0, this.firstChunkMinSize),
                count: this.firstChunkMinSize,
                time: timeTaken
              });
            }
          }
          continue;
        }
        
        // Progressive chunk handling (after first chunk is loaded)
        this.currentChunk.push(parsedRow);
        
        if (this.currentChunk.length >= this.progressiveChunkSize) {
          this._sendProgressiveChunk(onProgressiveChunkLoaded);
        }
      } catch (e) {
        console.error('Error parsing JSON line:', line);
      }
    }
  }
  
  _sendProgressiveChunk(onProgressiveChunkLoaded) {
    if (this.currentChunk.length === 0 || !onProgressiveChunkLoaded) return;
    
    const timeTaken = (Date.now() - this.loadStartTime) / 1000;
    
    onProgressiveChunkLoaded({
      rows: this.currentChunk,
      count: this.currentChunk.length,
      totalCount: this.totalRowCount,
      time: timeTaken
    });
    
    // Reset current chunk buffer
    this.currentChunk = [];
  }
  
  getAllData() {
    return this.allData;
  }
  
  getFirstChunkData() {
    return this.allData.slice(0, this.firstChunkMinSize);
  }
}