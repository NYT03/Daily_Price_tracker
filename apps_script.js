const FETCH_TIMES = ["09:15", "11:00", "12:30", "14:00", "15:45"];

function doPost(e) {
  try {
    // Attempt to parse the incoming data from Vercel
    const payload = JSON.parse(e.postData.contents);
    const targetDate = payload.date; // e.g., "2026-04-16"
    const targetTimeSlot = payload.time_slot; // e.g., "09:15"
    const marketData = payload.data; 

    // Retrieve the active spreadsheet
    const ss = SpreadsheetApp.getActiveSpreadsheet();

    // Iterate over the companies in the parsed payload
    for (let symbol in marketData) {
      let sheet = ss.getSheetByName(symbol);
      
      // If a tab for the specific company doesn't exist, visually build it!
      if (!sheet) {
        sheet = ss.insertSheet(symbol);
        let headers = ["Date"].concat(FETCH_TIMES);
        sheet.appendRow(headers);
        sheet.getRange(1, 1, 1, headers.length).setFontWeight("bold");
      }

      let dataRef = marketData[symbol];
      let price = dataRef.price.toFixed(2);
      let vol = dataRef.volume.toLocaleString('en-US'); // Format volume seamlessly
      let cellValue = vol + " | " + price;

      // Find the row for the targetDate
      let dataRange = sheet.getDataRange().getValues();
      let rowIndex = -1;
      
      for (let i = 0; i < dataRange.length; i++) {
        if (dataRange[i][0] == targetDate) {
          rowIndex = i + 1; // getRange uses 1-based indexing for rows
          break;
        }
      }

      // If we didn't find the date, append a new row automatically
      if (rowIndex === -1) {
        rowIndex = sheet.getLastRow() + 1;
        sheet.getRange(rowIndex, 1).setValue(targetDate);
      }

      // Find exactly which column index handles the selected FETCH_TIME
      let colIndex = FETCH_TIMES.indexOf(targetTimeSlot) + 2; 

      // Push the data safely to the target cell
      sheet.getRange(rowIndex, colIndex).setValue(cellValue);
    }
    
    // Return a valid JSON back to Vercel stating success
    return ContentService.createTextOutput(JSON.stringify({"status": "success"}))
                         .setMimeType(ContentService.MimeType.JSON);
    
  } catch (error) {
    // Failsafe error handling
    return ContentService.createTextOutput(JSON.stringify({"status": "error", "message": error.toString()}))
                         .setMimeType(ContentService.MimeType.JSON);
  }
}
