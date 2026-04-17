function doPost(e) {
  try {
    const payload = JSON.parse(e.postData.contents);
    const targetDate = payload.date; // e.g. "2026-04-16"
    const targetTimeSlot = payload.time_slot;
    const companies = payload.companies;
    const fetch_times = payload.fetch_times;
    const marketData = payload.data; 

    const ss = SpreadsheetApp.getActiveSpreadsheet();
    let sheet = ss.getSheetByName("Data");
    
    // 1. Build or Rebuild the ultra-wide layout if the symbols changed
    let rebuildHeaders = false;
    if (!sheet) {
      sheet = ss.insertSheet("Data");
      rebuildHeaders = true;
    } else {
      let currentHeaders = sheet.getRange(1, 1, 1, Math.max(1, sheet.getLastColumn())).getValues()[0];
      companies.forEach((company, cIndex) => {
          let offset = 1 + (cIndex * fetch_times.length * 2); // zero-based index for array
          if (offset >= currentHeaders.length || currentHeaders[offset] !== company) {
              rebuildHeaders = true;
          }
      });
    }

    if (rebuildHeaders) {
      let requiredCols = 2 + (companies.length * fetch_times.length * 2);
      if (sheet.getMaxColumns() < requiredCols) {
         sheet.insertColumnsAfter(sheet.getMaxColumns(), requiredCols - sheet.getMaxColumns());
      }

      let row1 = [""]; // Header A1
      let row2 = ["Date"]; // Header A2
      let row3 = [""]; // Header A3
      
      companies.forEach((company) => {
         row1.push(company);
         for(let i=1; i < (fetch_times.length * 2); i++) {
           row1.push("");
         }
         
         fetch_times.forEach(time => {
            row2.push(time);
            row2.push(""); // Padding for time merge
            row3.push("Price");
            row3.push("Traded Volume");
         });
      });
      
      if (sheet.getLastRow() > 0) {
         sheet.getRange(1, 1, 2, sheet.getMaxColumns()).breakApart();
      }
      
      sheet.getRange(1, 1, 1, row1.length).setValues([row1]);
      sheet.getRange(2, 1, 1, row2.length).setValues([row2]);
      sheet.getRange(3, 1, 1, row3.length).setValues([row3]);
      
      // Formatting and Merging
      sheet.getRange(1, 1, 3, sheet.getMaxColumns()).setFontWeight("bold").setHorizontalAlignment("center");
      
      companies.forEach((company, cIndex) => {
         let cStartCol = 2 + (cIndex * fetch_times.length * 2);
         sheet.getRange(1, cStartCol, 1, fetch_times.length * 2).mergeAcross();
         
         fetch_times.forEach((time, tIndex) => {
            let tStartCol = cStartCol + (tIndex * 2);
            sheet.getRange(2, tStartCol, 1, 2).mergeAcross();
         });
      });
      
      // Freeze the first 3 rows and 1st column so it is readable when scrolling
      sheet.setFrozenRows(3);
      sheet.setFrozenColumns(1);
    }

    // 2. Locate the row pointing to our targetDate
    let dataRange = sheet.getDataRange().getValues();
    let rowIndex = -1;
    
    for (let i = 3; i < dataRange.length; i++) { // Starting after Header Row 3
      let cellVal = dataRange[i][0];
      let cellDateStr = cellVal;
      
      // Handle instances where sheets parses it natively as an object
      if (cellVal instanceof Date) {
        let y = cellVal.getFullYear();
        let m = ('0' + (cellVal.getMonth() + 1)).slice(-2);
        let d = ('0' + cellVal.getDate()).slice(-2);
        cellDateStr = `${y}-${m}-${d}`;
      } else {
        cellDateStr = String(cellVal).trim();
      }
      
      if (cellDateStr === targetDate) {
        rowIndex = i + 1; // getRange uses 1-based indexing
        break;
      }
    }

    // If Date is not found, dynamically append a new line matching the targetDate column
    if (rowIndex === -1) {
      rowIndex = sheet.getLastRow() + 1;
      sheet.getRange(rowIndex, 1).setValue(targetDate);
    }

    // 3. Write data intelligently mapped across the correct column offsets
    let timeIndex = fetch_times.indexOf(targetTimeSlot);
    if (timeIndex !== -1) {
       let requiredCols = 2 + (companies.length * fetch_times.length * 2);
       if (sheet.getMaxColumns() < requiredCols) {
          sheet.insertColumnsAfter(sheet.getMaxColumns(), requiredCols - sheet.getMaxColumns());
       }
       
       let rowRange = sheet.getRange(rowIndex, 1, 1, requiredCols);
       let rowValues = rowRange.getValues()[0];
       
       companies.forEach((company, cIndex) => {
          let cBaseColumn = 2 + (cIndex * fetch_times.length * 2); // Which major column this company starts
          let targetColumn = cBaseColumn + (timeIndex * 2); // Exact mapped column for Price & Volume layout
          
          if (marketData[company] && !marketData[company].error) {
             let price = marketData[company].price;
             let vol = marketData[company].volume.toLocaleString('en-US'); // Add commas natively
             
             // -1 because array is 0-indexed
             rowValues[targetColumn - 1] = price;
             rowValues[targetColumn] = vol;
          }
       });
       
       rowRange.setValues([rowValues]);
    }

    // Output JSON cleanly for Vercel
    return ContentService.createTextOutput(JSON.stringify({"status": "success"})).setMimeType(ContentService.MimeType.JSON);
    
  } catch (error) {
    return ContentService.createTextOutput(JSON.stringify({"status": "error", "message": error.toString()})).setMimeType(ContentService.MimeType.JSON);
  }
}
