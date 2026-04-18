function doPost(e) {
  try {
    var data = JSON.parse(e.postData.contents);
    var ss = SpreadsheetApp.getActiveSpreadsheet();
    var sheet = ss.getSheetByName("us Screener");
    
    if (!sheet) { sheet = ss.insertSheet("us Screener"); }

    sheet.clear();
    // 写入数据
    sheet.getRange(1, 1, data.length, data[0].length).setValues(data);

    // 调用美化插件
    applyProfessionalFormatting(sheet, data.length, data[0].length);

    return ContentService.createTextOutput("Success");
  } catch (err) {
    return ContentService.createTextOutput("Error: " + err.toString());
  }
}

function applyProfessionalFormatting(sheet, rows, cols) {
  // 1. 设置行高（增加垂直间距）
  sheet.setRowHeight(1, 35); // 表头高度
  sheet.setRowHeights(2, rows - 1, 28); // 数据行高度，让文字不拥挤

  // 2. 核心：设置列宽 (按图11.30 比例精调)
  // [A:Ticker, B:Industry, C:Score, D:Action, E:Resonance, F:ADR, G:Vol, H:Bias, I:MktCap...]
  var widths = [70, 160, 60, 130, 90, 70, 80, 70, 90, 80, 70, 80, 75, 75, 75, 75, 75];
  for (var i = 0; i < widths.length; i++) {
    sheet.setColumnWidth(i + 1, widths[i]);
  }

  // 3. 全局文字样式
  var fullRange = sheet.getRange(1, 1, rows, cols);
  fullRange.setFontFamily("Roboto")
           .setVerticalAlignment("middle")
           .setHorizontalAlignment("center")
           .setFontSize(10);

  // 4. 表头样式 (深色背景，白字)
  var headerRange = sheet.getRange(1, 1, 1, cols);
  headerRange.setBackground("#333333")
             .setFontColor("#FFFFFF")
             .setFontWeight("bold")
             .setFontSize(11);

  // 5. 自动冻结表头
  sheet.setFrozenRows(1);

  // 6. 条件格式：Action 建议美化 (可选)
  // 逻辑：如果包含 STRONG 则字体加粗
  var actionRange = sheet.getRange(2, 4, rows - 1, 1);
  var rule = SpreadsheetApp.newConditionalFormatRule()
    .whenTextContains("STRONG")
    .setFontWeight("bold")
    .setFontColor("#d93025") // 红色加粗显示
    .setRanges([actionRange])
    .build();
  
  var rules = sheet.getConditionalFormatRules();
  rules.push(rule);
  sheet.setConditionalFormatRules(rules);

  // 7. 斑马纹交替背景
  sheet.getRange(2, 1, rows - 1, cols).setBackground(null); // 先清除
  for (var r = 2; r <= rows; r++) {
    if (r % 2 == 0) {
      sheet.getRange(r, 1, 1, cols).setBackground("#f8f9fa");
    }
  }
}
