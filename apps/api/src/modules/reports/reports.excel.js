// src/modules/reports/reports.excel.js
import ExcelJS from 'exceljs';

// ─────────────────────────────────────────────────────────────────────────────
// TEMA VISUAL — colores y fuentes del sistema PACIOLI
// ─────────────────────────────────────────────────────────────────────────────
const THEME = {
  headerBg:    '1E3A5F',   // Azul oscuro para título del reporte
  headerText:  'FFFFFF',   // Blanco
  colHeaderBg: '2E6DA4',   // Azul medio para encabezados de columnas
  colHeaderFg: 'FFFFFF',
  subHeaderBg: 'D9E1F2',   // Azul muy claro para metadatos
  subHeaderFg: '1E3A5F',
  rowAlt:      'F2F7FF',   // Azul casi blanco para filas alternas
  totalBg:     'E8F0FE',   // Azul claro para fila de totales
  totalFg:     '1E3A5F',
  borderColor: 'B8CCE4',
};

// Columnas numéricas que deben tener suma en la fila de totales
const NUMERIC_COLS = new Set([
  'amount_total', 'amount_outstanding', 'conciliable_amount',
  'amount_gross', 'amount_net', 'amount_commission',
  'amount_tax_iva', 'amount_tax_irf',
  'final_amount_gross', 'final_amount_net', 'final_amount_commission',
  'final_amount_tax_iva', 'final_amount_tax_irf',
  'financial_amount_gross', 'financial_amount_net', 'financial_commission',
  'financial_tax_iva', 'financial_tax_irf',
  'diff_adjustment',
]);

// ─────────────────────────────────────────────────────────────────────────────
// HELPER — Aplicar estilo a una celda
// ─────────────────────────────────────────────────────────────────────────────
function styleCell(cell, { bgColor, fontColor, bold = false, size = 10, align = 'left', wrap = false } = {}) {
  if (bgColor) {
    cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: `FF${bgColor}` } };
  }
  cell.font = {
    name:  'Calibri',
    size,
    bold,
    color: fontColor ? { argb: `FF${fontColor}` } : { argb: 'FF000000' },
  };
  cell.alignment = { horizontal: align, vertical: 'middle', wrapText: wrap };
}

function styleBorder(cell) {
  const border = { style: 'thin', color: { argb: `FF${THEME.borderColor}` } };
  cell.border  = { top: border, left: border, bottom: border, right: border };
}

// ─────────────────────────────────────────────────────────────────────────────
// HELPER — Escribe el encabezado estándar del reporte (filas 1-5)
// Retorna el número de fila donde empiezan los datos
// ─────────────────────────────────────────────────────────────────────────────
function writeReportHeader(sheet, reportName, startDate, endDate, colCount) {
  // Fila 1 — Nombre del sistema
  sheet.mergeCells(1, 1, 1, colCount);
  const titleCell = sheet.getCell(1, 1);
  titleCell.value = 'PACIOLI — BalanceIQ Reconciliation System';
  styleCell(titleCell, { bgColor: THEME.headerBg, fontColor: THEME.headerText, bold: true, size: 13, align: 'center' });
  sheet.getRow(1).height = 28;

  // Fila 2 — Nombre del reporte
  sheet.mergeCells(2, 1, 2, colCount);
  const nameCell = sheet.getCell(2, 1);
  nameCell.value = reportName;
  styleCell(nameCell, { bgColor: THEME.headerBg, fontColor: THEME.headerText, bold: true, size: 11, align: 'center' });
  sheet.getRow(2).height = 22;

  // Fila 3 — Período
  sheet.mergeCells(3, 1, 3, colCount);
  const periodCell = sheet.getCell(3, 1);
  periodCell.value = `Period: ${startDate}  →  ${endDate}`;
  styleCell(periodCell, { bgColor: THEME.subHeaderBg, fontColor: THEME.subHeaderFg, size: 10, align: 'center' });
  sheet.getRow(3).height = 18;

  // Fila 4 — Fecha de generación
  sheet.mergeCells(4, 1, 4, colCount);
  const genCell = sheet.getCell(4, 1);
  genCell.value = `Generated: ${new Date().toLocaleString('en-US', { dateStyle: 'medium', timeStyle: 'short' })}`;
  styleCell(genCell, { bgColor: THEME.subHeaderBg, fontColor: THEME.subHeaderFg, size: 9, align: 'center' });
  sheet.getRow(4).height = 16;

  // Fila 5 — vacía
  sheet.getRow(5).height = 6;

  return 6; // Los encabezados de columnas van en fila 6
}

// ─────────────────────────────────────────────────────────────────────────────
// HELPER — Escribe encabezados de columnas y los datos
// ─────────────────────────────────────────────────────────────────────────────
function writeDataSheet(sheet, reportName, startDate, endDate, columns, rows) {
  const colCount  = columns.length;
  const headerRow = writeReportHeader(sheet, reportName, startDate, endDate, colCount);

  // Encabezados de columnas (fila 6)
  const hRow = sheet.getRow(headerRow);
  hRow.height = 20;
  columns.forEach((col, i) => {
    const cell = hRow.getCell(i + 1);
    cell.value = col.header;
    styleCell(cell, { bgColor: THEME.colHeaderBg, fontColor: THEME.colHeaderFg, bold: true, size: 9, align: 'center' });
    styleBorder(cell);
  });

  // Ancho de columnas
  sheet.columns = columns.map(col => ({
    key:   col.key,
    width: col.width || 16,
  }));

  // Totales acumuladores
  const totals = {};
  columns.forEach(col => {
    if (NUMERIC_COLS.has(col.key)) totals[col.key] = 0;
  });

  // Filas de datos
  rows.forEach((row, idx) => {
    const dataRow    = sheet.getRow(headerRow + 1 + idx);
    const isAlt      = idx % 2 === 1;
    dataRow.height   = 16;

    columns.forEach((col, ci) => {
      const cell  = dataRow.getCell(ci + 1);
      let value   = row[col.key];

      // Formatear valores
      if (value === null || value === undefined) {
        cell.value = '—';
      } else if (col.type === 'date' && value) {
        cell.value = new Date(value).toLocaleDateString('en-US');
      } else if (col.type === 'numeric' || NUMERIC_COLS.has(col.key)) {
        const num  = parseFloat(value) || 0;
        cell.value = num;
        cell.numFmt = '#,##0.00';
        if (totals[col.key] !== undefined) totals[col.key] += num;
      } else if (col.type === 'boolean') {
        cell.value = value ? 'Yes' : 'No';
      } else {
        cell.value = String(value);
      }

      styleCell(cell, {
        bgColor: isAlt ? THEME.rowAlt : undefined,
        size:    9,
        align:   NUMERIC_COLS.has(col.key) ? 'right' : 'left',
      });
      styleBorder(cell);
    });
  });

  // Fila de totales
  const hasNumeric = Object.keys(totals).length > 0;
  if (hasNumeric && rows.length > 0) {
    const totalRowNum = headerRow + 1 + rows.length;
    const totalRow    = sheet.getRow(totalRowNum);
    totalRow.height   = 18;

    columns.forEach((col, ci) => {
      const cell = totalRow.getCell(ci + 1);
      if (ci === 0) {
        cell.value = `TOTAL  (${rows.length} rows)`;
        styleCell(cell, { bgColor: THEME.totalBg, fontColor: THEME.totalFg, bold: true, size: 9 });
      } else if (totals[col.key] !== undefined) {
        cell.value  = totals[col.key];
        cell.numFmt = '#,##0.00';
        styleCell(cell, { bgColor: THEME.totalBg, fontColor: THEME.totalFg, bold: true, size: 9, align: 'right' });
      } else {
        styleCell(cell, { bgColor: THEME.totalBg });
      }
      styleBorder(cell);
    });
  }

  // Freeze panes — fijar las primeras 6 filas (encabezado) + columna 1
  sheet.views = [{ state: 'frozen', xSplit: 1, ySplit: headerRow, activeCell: 'B7' }];
}

// ─────────────────────────────────────────────────────────────────────────────
// DEFINICIÓN DE COLUMNAS POR REPORTE
// ─────────────────────────────────────────────────────────────────────────────
const COLUMNS = {
  bank: [
    { key: 'stg_id',                  header: 'STG ID',              width: 10 },
    { key: 'doc_date',                header: 'Doc Date',            width: 12, type: 'date' },
    { key: 'doc_type',                header: 'Doc Type',            width: 8  },
    { key: 'amount_total',            header: 'Amount',              width: 14, type: 'numeric' },
    { key: 'sap_description',         header: 'SAP Description',     width: 30 },
    { key: 'bank_ref_2',              header: 'Bank Ref',            width: 18 },
    { key: 'trans_type',              header: 'Trans Type',          width: 20 },
    { key: 'global_category',         header: 'Category',            width: 25 },
    { key: 'brand',                   header: 'Brand',               width: 14 },
    { key: 'batch_number',            header: 'Batch',               width: 14 },
    { key: 'match_hash_key',          header: 'Hash Key',            width: 20 },
    { key: 'reconcile_status',        header: 'Status',              width: 16 },
    { key: 'settlement_id',           header: 'Settlement ID',       width: 20 },
    { key: 'establishment_name',      header: 'Establishment',       width: 22 },
    { key: 'count_voucher_bank',      header: 'Vouchers Bank',       width: 12, type: 'numeric' },
    { key: 'count_voucher_portfolio', header: 'Vouchers Portfolio',  width: 14, type: 'numeric' },
    { key: 'final_amount_gross',      header: 'Gross',               width: 14, type: 'numeric' },
    { key: 'final_amount_net',        header: 'Net',                 width: 14, type: 'numeric' },
    { key: 'final_amount_commission', header: 'Commission',          width: 14, type: 'numeric' },
    { key: 'final_amount_tax_iva',    header: 'Tax IVA',             width: 12, type: 'numeric' },
    { key: 'final_amount_tax_irf',    header: 'Tax IRF',             width: 12, type: 'numeric' },
    { key: 'diff_adjustment',         header: 'Diff Adj.',           width: 12, type: 'numeric' },
    { key: 'reconcile_reason',        header: 'Reason',              width: 20 },
    { key: 'enrich_customer_id',      header: 'Customer ID',         width: 12 },
    { key: 'enrich_customer_name',    header: 'Customer Name',       width: 28 },
    { key: 'enrich_notes',            header: 'Notes',               width: 30 },
  ],

  portfolio: [
    { key: 'stg_id',                  header: 'STG ID',          width: 10 },
    { key: 'customer_code',           header: 'Customer Code',   width: 14 },
    { key: 'customer_name',           header: 'Customer Name',   width: 28 },
    { key: 'assignment',              header: 'Assignment',      width: 16 },
    { key: 'invoice_ref',             header: 'Invoice Ref',     width: 18 },
    { key: 'doc_date',                header: 'Doc Date',        width: 12, type: 'date' },
    { key: 'due_date',                header: 'Due Date',        width: 12, type: 'date' },
    { key: 'amount_outstanding',      header: 'Outstanding',     width: 14, type: 'numeric' },
    { key: 'conciliable_amount',      header: 'Conciliable',     width: 14, type: 'numeric' },
    { key: 'enrich_batch',            header: 'Enrich Batch',    width: 14 },
    { key: 'enrich_ref',              header: 'Enrich Ref',      width: 14 },
    { key: 'enrich_brand',            header: 'Brand',           width: 12 },
    { key: 'enrich_user',             header: 'Enrich User',     width: 14 },
    { key: 'enrich_source',           header: 'Source',          width: 14 },
    { key: 'reconcile_group',         header: 'Group',           width: 16 },
    { key: 'match_hash_key',          header: 'Hash Key',        width: 20 },
    { key: 'reconcile_status',        header: 'Status',          width: 14 },
    { key: 'settlement_id',           header: 'Settlement ID',   width: 20 },
    { key: 'financial_amount_gross',  header: 'Gross',           width: 14, type: 'numeric' },
    { key: 'financial_amount_net',    header: 'Net',             width: 14, type: 'numeric' },
    { key: 'financial_commission',    header: 'Commission',      width: 14, type: 'numeric' },
    { key: 'financial_tax_iva',       header: 'Tax IVA',         width: 12, type: 'numeric' },
    { key: 'financial_tax_irf',       header: 'Tax IRF',         width: 12, type: 'numeric' },
    { key: 'match_method',            header: 'Match Method',    width: 16 },
  ],

  'card-details': [
    { key: 'stg_id',           header: 'STG ID',          width: 10 },
    { key: 'settlement_id',    header: 'Settlement ID',   width: 20 },
    { key: 'voucher_date',     header: 'Voucher Date',    width: 13, type: 'date' },
    { key: 'card_number',      header: 'Card Number',     width: 18 },
    { key: 'auth_code',        header: 'Auth Code',       width: 12 },
    { key: 'voucher_ref',      header: 'Voucher Ref',     width: 16 },
    { key: 'batch_number',     header: 'Batch',           width: 14 },
    { key: 'amount_gross',     header: 'Gross',           width: 14, type: 'numeric' },
    { key: 'amount_net',       header: 'Net',             width: 14, type: 'numeric' },
    { key: 'amount_commission',header: 'Commission',      width: 14, type: 'numeric' },
    { key: 'amount_tax_iva',   header: 'Tax IVA',         width: 12, type: 'numeric' },
    { key: 'amount_tax_irf',   header: 'Tax IRF',         width: 12, type: 'numeric' },
    { key: 'brand',            header: 'Brand',           width: 14 },
    { key: 'establishment_code',  header: 'Est. Code',    width: 12 },
    { key: 'establishment_name',  header: 'Establishment',width: 22 },
    { key: 'voucher_hash_key', header: 'Hash Key',        width: 20 },
    { key: 'reconcile_status', header: 'Status',          width: 14 },
  ],

  'card-settlements': [
    { key: 'stg_id',           header: 'STG ID',          width: 10 },
    { key: 'settlement_id',    header: 'Settlement ID',   width: 20 },
    { key: 'settlement_date',  header: 'Settlement Date', width: 14, type: 'date' },
    { key: 'brand',            header: 'Brand',           width: 14 },
    { key: 'batch_number',     header: 'Batch',           width: 14 },
    { key: 'amount_gross',     header: 'Gross',           width: 14, type: 'numeric' },
    { key: 'amount_net',       header: 'Net',             width: 14, type: 'numeric' },
    { key: 'amount_commission',header: 'Commission',      width: 14, type: 'numeric' },
    { key: 'amount_tax_iva',   header: 'Tax IVA',         width: 12, type: 'numeric' },
    { key: 'amount_tax_irf',   header: 'Tax IRF',         width: 12, type: 'numeric' },
    { key: 'match_hash_key',   header: 'Hash Key',        width: 20 },
    { key: 'reconcile_status', header: 'Status',          width: 14 },
    { key: 'count_voucher',    header: 'Vouchers',        width: 10, type: 'numeric' },
    { key: 'establishment_name', header: 'Establishment', width: 22 },
  ],

  parking: [
    { key: 'stg_id',           header: 'STG ID',          width: 10 },
    { key: 'settlement_date',  header: 'Settlement Date', width: 14, type: 'date' },
    { key: 'settlement_id',    header: 'Settlement ID',   width: 20 },
    { key: 'batch_number',     header: 'Batch',           width: 14 },
    { key: 'brand',            header: 'Brand',           width: 14 },
    { key: 'amount_gross',     header: 'Gross',           width: 14, type: 'numeric' },
    { key: 'amount_commission',header: 'Commission',      width: 14, type: 'numeric' },
    { key: 'amount_tax_iva',   header: 'Tax IVA',         width: 12, type: 'numeric' },
    { key: 'amount_tax_irf',   header: 'Tax IRF',         width: 12, type: 'numeric' },
    { key: 'amount_net',       header: 'Net',             width: 14, type: 'numeric' },
    { key: 'count_voucher',    header: 'Vouchers',        width: 10, type: 'numeric' },
    { key: 'match_hash_key',   header: 'Hash Key',        width: 20 },
    { key: 'reconcile_status', header: 'Status',          width: 14 },
  ],
};

// ─────────────────────────────────────────────────────────────────────────────
// EXPORTS PÚBLICOS
// ─────────────────────────────────────────────────────────────────────────────

export async function buildExcel(reportType, reportName, startDate, endDate, rows) {
  const wb   = new ExcelJS.Workbook();
  wb.creator = 'PACIOLI BalanceIQ';
  wb.created = new Date();

  const sheet = wb.addWorksheet(reportName.substring(0, 31));
  const cols  = COLUMNS[reportType];

  if (!cols) throw new Error(`Unknown report type: ${reportType}`);

  writeDataSheet(sheet, reportName, startDate, endDate, cols, rows);
  return wb;
}

export async function buildSummaryExcel(startDate, endDate, data) {
  const wb   = new ExcelJS.Workbook();
  wb.creator = 'PACIOLI BalanceIQ';
  wb.created = new Date();

  const sheet = wb.addWorksheet('Reconciliation Summary');
  const COLS  = 5;

  // Encabezado
  let row = writeReportHeader(sheet, 'Reconciliation Summary', startDate, endDate, COLS);

  // ── Sección Banco ───────────────────────────────────────────────────────
  const bankHeader = sheet.getRow(row);
  sheet.mergeCells(row, 1, row, COLS);
  const bhCell = bankHeader.getCell(1);
  bhCell.value = 'BANK TRANSACTIONS';
  styleCell(bhCell, { bgColor: THEME.colHeaderBg, fontColor: THEME.colHeaderFg, bold: true, size: 10, align: 'center' });
  bankHeader.height = 20;
  row++;

  // Sub-encabezados banco
  ['Status', 'Count', 'Total Amount', '', ''].forEach((h, i) => {
    const c = sheet.getRow(row).getCell(i + 1);
    c.value = h;
    styleCell(c, { bgColor: THEME.subHeaderBg, fontColor: THEME.subHeaderFg, bold: true, size: 9 });
    styleBorder(c);
  });
  sheet.getRow(row).height = 16;
  row++;

  let bankTotal = 0;
  let bankCount = 0;
  data.bank.forEach((b, idx) => {
    const r = sheet.getRow(row + idx);
    r.height = 15;
    const isAlt = idx % 2 === 1;
    [b.reconcile_status, parseInt(b.count), parseFloat(b.total_amount)].forEach((v, ci) => {
      const c = r.getCell(ci + 1);
      c.value = v;
      if (ci === 2) { c.numFmt = '#,##0.00'; }
      styleCell(c, { bgColor: isAlt ? THEME.rowAlt : undefined, size: 9,
                     align: ci > 0 ? 'right' : 'left' });
      styleBorder(c);
    });
    bankTotal += parseFloat(b.total_amount) || 0;
    bankCount += parseInt(b.count) || 0;
  });
  row += data.bank.length;

  // Total banco
  const bankTotalRow = sheet.getRow(row);
  bankTotalRow.height = 16;
  ['TOTAL', bankCount, bankTotal].forEach((v, ci) => {
    const c = bankTotalRow.getCell(ci + 1);
    c.value = v;
    if (ci === 2) c.numFmt = '#,##0.00';
    styleCell(c, { bgColor: THEME.totalBg, fontColor: THEME.totalFg, bold: true, size: 9,
                   align: ci > 0 ? 'right' : 'left' });
    styleBorder(c);
  });
  row += 2;

  // ── Sección Cartera ─────────────────────────────────────────────────────
  sheet.mergeCells(row, 1, row, COLS);
  const phCell = sheet.getRow(row).getCell(1);
  phCell.value = 'PORTFOLIO (CARTERA)';
  styleCell(phCell, { bgColor: THEME.colHeaderBg, fontColor: THEME.colHeaderFg, bold: true, size: 10, align: 'center' });
  sheet.getRow(row).height = 20;
  row++;

  ['Status', 'Count', 'Conciliable Amount', '', ''].forEach((h, i) => {
    const c = sheet.getRow(row).getCell(i + 1);
    c.value = h;
    styleCell(c, { bgColor: THEME.subHeaderBg, fontColor: THEME.subHeaderFg, bold: true, size: 9 });
    styleBorder(c);
  });
  sheet.getRow(row).height = 16;
  row++;

  let portTotal = 0;
  let portCount = 0;
  data.portfolio.forEach((p, idx) => {
    const r = sheet.getRow(row + idx);
    r.height = 15;
    const isAlt = idx % 2 === 1;
    [p.reconcile_status, parseInt(p.count), parseFloat(p.total_amount)].forEach((v, ci) => {
      const c = r.getCell(ci + 1);
      c.value = v;
      if (ci === 2) c.numFmt = '#,##0.00';
      styleCell(c, { bgColor: isAlt ? THEME.rowAlt : undefined, size: 9,
                     align: ci > 0 ? 'right' : 'left' });
      styleBorder(c);
    });
    portTotal += parseFloat(p.total_amount) || 0;
    portCount += parseInt(p.count) || 0;
  });
  row += data.portfolio.length;

  const portTotalRow = sheet.getRow(row);
  portTotalRow.height = 16;
  ['TOTAL', portCount, portTotal].forEach((v, ci) => {
    const c = portTotalRow.getCell(ci + 1);
    c.value = v;
    if (ci === 2) c.numFmt = '#,##0.00';
    styleCell(c, { bgColor: THEME.totalBg, fontColor: THEME.totalFg, bold: true, size: 9,
                   align: ci > 0 ? 'right' : 'left' });
    styleBorder(c);
  });
  row += 2;

  // ── Sección Tarjetas ────────────────────────────────────────────────────
  sheet.mergeCells(row, 1, row, COLS);
  const chCell = sheet.getRow(row).getCell(1);
  chCell.value = 'CARD SETTLEMENTS (BY BRAND)';
  styleCell(chCell, { bgColor: THEME.colHeaderBg, fontColor: THEME.colHeaderFg, bold: true, size: 10, align: 'center' });
  sheet.getRow(row).height = 20;
  row++;

  ['Brand', 'Status', 'Count', 'Gross Amount', 'Net Amount'].forEach((h, i) => {
    const c = sheet.getRow(row).getCell(i + 1);
    c.value = h;
    styleCell(c, { bgColor: THEME.subHeaderBg, fontColor: THEME.subHeaderFg, bold: true, size: 9 });
    styleBorder(c);
  });
  sheet.getRow(row).height = 16;
  row++;

  data.cards.forEach((cd, idx) => {
    const r = sheet.getRow(row + idx);
    r.height = 15;
    const isAlt = idx % 2 === 1;
    [cd.brand, cd.reconcile_status, parseInt(cd.count),
     parseFloat(cd.total_gross), parseFloat(cd.total_net)].forEach((v, ci) => {
      const c = r.getCell(ci + 1);
      c.value = v;
      if (ci >= 3) c.numFmt = '#,##0.00';
      styleCell(c, { bgColor: isAlt ? THEME.rowAlt : undefined, size: 9,
                     align: ci >= 2 ? 'right' : 'left' });
      styleBorder(c);
    });
  });

  // Ancho de columnas para el summary
  sheet.getColumn(1).width = 20;
  sheet.getColumn(2).width = 18;
  sheet.getColumn(3).width = 10;
  sheet.getColumn(4).width = 16;
  sheet.getColumn(5).width = 16;

  return wb;
}

// Genera el nombre del archivo para la descarga
export function buildFileName(reportType, startDate, endDate) {
  const names = {
    overview:          'Overview',
    bank:              'BankReconciliation',
    portfolio:         'Portfolio',
    'card-details':    'CardDetails',
    'card-settlements':'CardSettlements',
    parking:           'ParkingBreakdown',
    summary:           'ReconciliationSummary',
  };
  const name = names[reportType] || reportType;
  return `PACIOLI_${name}_${startDate}_${endDate}.xlsx`;
}