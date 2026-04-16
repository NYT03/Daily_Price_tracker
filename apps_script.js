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
    
    // 1. Build the ultra-wide layout if it does not exist
    if (!sheet) {
      sheet = ss.insertSheet("Data");
      
      let row1 = [""]; // Header A1
      let row2 = ["Date"]; // Header A2
      let row3 = [""]; // Header A3
      
      companies.forEach((company) => {
         row1.push(company);
         // Padding empty columns for the rest of this company's block (so we can merge later)
         // Each time slot takes 2 columns (Price + Volume), so (fetch_times.length * 2) - 1 empty padding needed
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
      
      sheet.appendRow(row1);
      sheet.appendRow(row2);
      sheet.appendRow(row3);
      
      // Formatting and Merging
      // Apply Bold
      sheet.getRange(1, 1, 3, sheet.getLastColumn()).setFontWeight("bold").setHorizontalAlignment("center");
      
      companies.forEach((company, cIndex) => {
         let cStartCol = 2 + (cIndex * fetch_times.length * 2);
         // Merge Company Name across all its time slots
         sheet.getRange(1, cStartCol, 1, fetch_times.length * 2).mergeAcross();
         
         fetch_times.forEach((time, tIndex) => {
            let tStartCol = cStartCol + (tIndex * 2);
            // Merge the specific Time Slot across Price + Volume columns
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
       companies.forEach((company, cIndex) => {
          let cBaseColumn = 2 + (cIndex * fetch_times.length * 2); // Which major column this company starts
          let targetColumn = cBaseColumn + (timeIndex * 2); // Exact mapped column for Price & Volume layout
          
          if (marketData[company] && !marketData[company].error) {
             let price = marketData[company].price;
             let vol = marketData[company].volume.toLocaleString('en-US'); // Add commas natively
             
             // Commit Value safely
             sheet.getRange(rowIndex, targetColumn).setValue(price);
             sheet.getRange(rowIndex, targetColumn + 1).setValue(vol);
          }
       });
    }

    // Output JSON cleanly for Vercel
    return ContentService.createTextOutput(JSON.stringify({"status": "success"})).setMimeType(ContentService.MimeType.JSON);
    
  } catch (error) {
    return ContentService.createTextOutput(JSON.stringify({"status": "error", "message": error.toString()})).setMimeType(ContentService.MimeType.JSON);
  }
}
