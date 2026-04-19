function doPost(e) {
  try {
    var data = JSON.parse(e.postData.contents);
    var ss = SpreadsheetApp.getActiveSpreadsheet();
    var sheet = ss.getSheetByName("us Screener") || ss.insertSheet("us Screener");
    
    // 【终极净化】清空旧数据、旧格式、旧规则，防止表格卡顿和幽灵乱码
    sheet.clear();
    sheet.clearFormats(); 
    sheet.clearConditionalFormatRules(); 
    
    if (data && data.length > 0) {
      sheet.getRange(1, 1, data.length, data[0].length).setValues(data);
      applyV20Format(sheet, data.length, data[0].length);
      return ContentService.createTextOutput("Success: Dashboard Updated with Colors");
    }
    return ContentService.createTextOutput("Error: No Data");
  } catch (err) {
    return ContentService.createTextOutput("Error: " + err.toString());
  }
}

function applyV20Format(sheet, rows, cols) {
  // 1. 仪表盘前3行 (去网格化，加外框)
  sheet.setRowHeights(1, 3, 30);
  var dashboard = sheet.getRange(1, 1, 3, 8); 
  dashboard.setBackground("#ffffff")
           .setBorder(true, true, true, true, false, false, "#2c3e50", SpreadsheetApp.BorderStyle.SOLID_MEDIUM)
           .setVerticalAlignment("middle");
  sheet.getRange("A1").setFontWeight("bold").setFontSize(12).setFontColor("#d32f2f"); 
  
  // 2. 核心表头第4行 (高级护眼绿)
  var header = sheet.getRange(4, 1, 1, cols);
  header.setBackground("#00e676") 
        .setFontColor("#000000")
        .setFontWeight("bold")
        .setHorizontalAlignment("center")
        .setVerticalAlignment("middle");
  sheet.setRowHeight(4, 35);

  // 3. 数据行排版
  if (rows > 4) {
    var dataRange = sheet.getRange(5, 1, rows - 4, cols);
    dataRange.setVerticalAlignment("middle").setHorizontalAlignment("center").setFontFamily("Roboto");
    sheet.setRowHeights(5, rows - 4, 30);
    
    // 强制量比(Vol_Ratio)为2位纯数字小数
    sheet.getRange(5, 7, rows - 4, 1).setNumberFormat("0.00");

    // ===== 🎨 彭博同款红绿智能高亮 =====
    var rules = [];
    
    // R20/R60 (列16, 17)：大于0绿+粗，小于0红+粗
    var rsRange = sheet.getRange(5, 16, rows - 4, 2);
    rules.push(SpreadsheetApp.newConditionalFormatRule().whenNumberGreaterThan(0).setFontColor("#0f9d58").setBold(true).setRanges([rsRange]).build());
    rules.push(SpreadsheetApp.newConditionalFormatRule().whenNumberLessThan(0).setFontColor("#d32f2f").setBold(true).setRanges([rsRange]).build());
    
    // 涨跌幅 5D/20D/60D (列13, 14, 15)：带负号的显红
    var pctRange = sheet.getRange(5, 13, rows - 4, 3);
    rules.push(SpreadsheetApp.newConditionalFormatRule().whenTextStartsWith("-").setFontColor("#d32f2f").setRanges([pctRange]).build());

    // 动作建议 (列4)：STRONG BUY 显红+粗
    var actionRange = sheet.getRange(5, 4, rows - 4, 1);
    rules.push(SpreadsheetApp.newConditionalFormatRule().whenTextContains("STRONG BUY").setFontColor("#d32f2f").setBold(true).setRanges([actionRange]).build());

    // 一次性应用所有规则 (防止卡顿)
    sheet.setConditionalFormatRules(rules);
  }

  // 4. 黄金比例列宽
  var widths = [80, 160, 60, 140, 90, 75, 80, 75, 90, 80, 70, 80, 80, 80, 80, 80, 80];
  for (var i = 0; i < widths.length; i++) {
    sheet.setColumnWidth(i + 1, widths[i] || 80);
  }
  
  // 冻结前4行，方便滚动
  sheet.setFrozenRows(4); 
}
