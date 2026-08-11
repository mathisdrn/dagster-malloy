/**
 * CLI helper script to extract model AST metadata using @malloydata/malloy compiler.
 * Usage: node parse_malloy_ast.js <path_to_malloy_or_malloynb_file>
 */

const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');
const { fileURLToPath } = require('url');

function loadMalloy() {
  try {
    return require('@malloydata/malloy');
  } catch (e) {}

  const candidates = [];
  try {
    const globalRoot = execSync('npm root -g', { encoding: 'utf-8' }).trim();
    if (globalRoot) {
      candidates.push(path.join(globalRoot, '@malloydata', 'malloy'));
      candidates.push(path.join(globalRoot, '@malloydata', 'cli', 'node_modules', '@malloydata', 'malloy'));
    }
  } catch (e) {}

  for (const cand of candidates) {
    try {
      if (fs.existsSync(cand)) {
        return require(cand);
      }
    } catch (e) {}
  }
  throw new Error('Could not resolve @malloydata/malloy package. Please run `npm install -g @malloydata/malloy`.');
}

const DASHBOARD_TAGS = new Set([
  'dashboard', 'bar_chart', 'line_chart', 'scatter_chart',
  'shape_map', 'segment_map', 'render', 'report', 'viz'
]);

function extractString(val) {
  if (!val) return null;
  if (typeof val === 'string') return val;
  if (typeof val.name === 'string') return val.name;
  if (typeof val.text === 'string') return val.text;
  if (val.ref && typeof val.ref.name === 'string') return val.ref.name;
  return null;
}

function extractQuerySourceName(qExpr) {
  if (!qExpr) return null;
  if (qExpr.elementType === 'sq-arrow') {
    return extractQuerySourceName(qExpr.applyTo);
  }
  if (qExpr.elementType === 'sq-reference') {
    return extractString(qExpr.ref);
  }
  return extractString(qExpr.ref) || extractString(qExpr.name) || extractString(qExpr);
}

function parseMalloyAST(filePath) {
  const absolutePath = path.resolve(filePath);
  if (!fs.existsSync(absolutePath)) {
    throw new Error(`File not found: ${absolutePath}`);
  }

  const isNotebook = absolutePath.endsWith('.malloynb');
  let rawContent = fs.readFileSync(absolutePath, 'utf-8');

  let codeToParse = rawContent;
  let notebookCellMap = new Map();

  if (isNotebook) {
    try {
      const nbJson = JSON.parse(rawContent);
      const notebookCells = nbJson.cells || [];
      const codeLines = [];
      let currentLineIdx = 0;

      notebookCells.forEach((cell, cellIdx) => {
        const cellKind = cell.cell_type || cell.kind;
        if (cellKind === 'code' || cellKind === 2) {
          const src = Array.isArray(cell.source) ? cell.source.join('') : String(cell.source || '');
          const lines = src.split('\n');
          lines.forEach(() => {
            notebookCellMap.set(currentLineIdx, cellIdx);
            currentLineIdx++;
          });
          codeLines.push(src);
          currentLineIdx++;
        }
      });
      codeToParse = codeLines.join('\n\n');
    } catch (e) {
      codeToParse = rawContent;
    }
  }

  const fileUrl = 'file://' + absolutePath;
  const { MalloyTranslator } = loadMalloy();
  const t = new MalloyTranslator(fileUrl);
  t.importZone.define(fileUrl, codeToParse);

  // Run parser step
  t.parseStep.step(t);

  const importedFiles = new Set();
  const tableDeps = new Set();

  // Loop to resolve imports, dialects, and table schemas
  for (let i = 0; i < 10; i++) {
    const res = t.importsAndTablesStep.step(t);
    if (!res) break;

    if (res.urls && Array.isArray(res.urls)) {
      res.urls.forEach(u => {
        let importPath = u;
        try {
          if (u.startsWith('file://')) {
            importPath = fileURLToPath(u);
          }
        } catch (e) {}

        const relName = path.basename(importPath);
        importedFiles.add(relName);

        if (fs.existsSync(importPath)) {
          try {
            const content = fs.readFileSync(importPath, 'utf-8');
            t.importZone.define(u, content);
          } catch (e) {
            t.importZone.define(u, '-- stub import');
          }
        } else {
          t.importZone.define(u, '-- stub import');
        }
      });
    }

    if (res.connectionDialects) {
      for (const dialectName of Object.keys(res.connectionDialects)) {
        t.connectionDialectZone.define(dialectName, dialectName);
      }
    }

    if (res.tables) {
      for (const [tableKey, tableInfo] of Object.entries(res.tables)) {
        if (tableInfo.tablePath) {
          let rawTable = tableInfo.tablePath.trim();
          if ((rawTable.startsWith("'") && rawTable.endsWith("'")) ||
              (rawTable.startsWith('"') && rawTable.endsWith('"'))) {
            rawTable = rawTable.slice(1, -1);
          }
          tableDeps.add(rawTable);
        }
        t.schemaZone.define(tableKey, {
          dialect: tableInfo.connectionName || 'duckdb',
          structRelationship: { type: 'basetable', connectionName: tableInfo.connectionName || 'duckdb' },
          fields: []
        });
      }
    }
  }

  // Run AST step
  const astRes = t.astStep.step(t);
  const doc = astRes ? astRes.ast : null;

  const sources = {};
  const queries = {};
  const codeLines = codeToParse.split('\n');

  function extractRawBlock(loc) {
    if (!loc || !loc.range) return '';
    const startLine = loc.range.start ? loc.range.start.line : 0;
    const endLine = loc.range.end ? loc.range.end.line : startLine;
    return codeLines.slice(startLine, endLine + 1).join('\n');
  }

  function extractPrecedingAnnotations(startLineIdx) {
    const tags = [];
    const descLines = [];
    let idx = startLineIdx - 1;
    const commentBlock = [];

    while (idx >= 0) {
      const line = codeLines[idx].trim();
      if (line.startsWith('#')) {
        commentBlock.unshift(line);
        idx--;
      } else {
        break;
      }
    }

    commentBlock.forEach(line => {
      const tagMatch = line.match(/^#\s*@([a-zA-Z0-9_-]+)/);
      if (tagMatch) {
        tags.push(tagMatch[1].toLowerCase());
      } else {
        const commentText = line.replace(/^#+\s*/, '').trim();
        if (commentText) descLines.push(commentText);
      }
    });

    return { tags, description: descLines.length > 0 ? descLines.join('\n') : null };
  }

  function inspectSourceExpr(se) {
    if (!se) return {};
    if (se.elementType === 'sq-extend') {
      return inspectSourceExpr(se.sqSrc);
    }
    if (se.elementType === 'sq-source' && se.theSource) {
      const tSrc = se.theSource;
      let rawTable = extractString(tSrc.tablePath);
      if (rawTable) {
        rawTable = rawTable.trim();
        if ((rawTable.startsWith("'") && rawTable.endsWith("'")) ||
            (rawTable.startsWith('"') && rawTable.endsWith('"'))) {
          rawTable = rawTable.slice(1, -1);
        }
      }
      return {
        connection: extractString(tSrc.connectionName),
        table_or_sql: rawTable
      };
    }
    if (se.elementType === 'sq-reference') {
      return {
        base_source_name: extractString(se.ref)
      };
    }
    return {};
  }

  if (doc && doc.statements && doc.statements.elements) {
    doc.statements.elements.forEach((stmtGroup) => {
      if (!stmtGroup.elements) return;
      stmtGroup.elements.forEach((el) => {
        const startLineIdx = (el.location && el.location.range && el.location.range.start)
          ? el.location.range.start.line
          : 0;
        const startLine = startLineIdx + 1;
        const rawCode = extractRawBlock(el.location);
        const { tags, description } = extractPrecedingAnnotations(startLineIdx);
        const cellIdx = isNotebook ? (notebookCellMap.get(startLineIdx) ?? 0) : null;

        if (el.elementType === 'defineSource' && el.name) {
          const joinedSources = new Set();
          if (rawCode) {
            const joinMatches = rawCode.matchAll(/join_(?:one|many|cross)\s*:\s*([a-zA-Z0-9_]+)/gi);
            for (const m of joinMatches) {
              joinedSources.add(m[1]);
            }
          }

          const srcDetails = inspectSourceExpr(el.sourceExpr);

          sources[el.name] = {
            name: el.name,
            connection: srcDetails.connection || null,
            table_or_sql: srcDetails.table_or_sql || null,
            base_source_name: srcDetails.base_source_name || null,
            line_number: startLine,
            raw_code: rawCode,
            joined_sources: Array.from(joinedSources)
          };
        } else if (el.elementType === 'defineQuery' && el.name) {
          const isCheck = tags.some(t => ['check', 'test', 'assert'].includes(t)) ||
            /^(check_|test_|assert_)/i.test(el.name);
          const isDashboard = tags.some(t => DASHBOARD_TAGS.has(t));

          const nestedViews = [];
          if (rawCode) {
            const nestMatches = rawCode.matchAll(/nest\s*:\s*([a-zA-Z0-9_]+)/gi);
            for (const m of nestMatches) {
              nestedViews.push(m[1]);
            }
          }

          let sourceName = extractQuerySourceName(el.queryExpr);
          let viewName = null;
          if (el.queryExpr && el.queryExpr.elementType === 'sq-arrow') {
            viewName = extractString(el.queryExpr.operation) || null;
          }

          queries[el.name] = {
            name: el.name,
            source_name: sourceName,
            view_name: viewName,
            description: description,
            line_number: startLine,
            raw_code: rawCode,
            is_check: isCheck,
            is_dashboard: isDashboard,
            is_notebook_cell: isNotebook,
            cell_index: cellIdx,
            tags: tags,
            nested_views: nestedViews
          };
        }
      });
    });
  }

  return {
    file_path: absolutePath,
    sources,
    queries,
    imports: Array.from(importedFiles),
    table_dependencies: Array.from(tableDeps)
  };
}

// Run CLI
if (require.main === module) {
  const filePath = process.argv[2];
  if (!filePath) {
    console.error('Usage: node parse_malloy_ast.js <file_path>');
    process.exit(1);
  }
  try {
    const result = parseMalloyAST(filePath);
    console.log(JSON.stringify(result, null, 2));
  } catch (err) {
    console.error(JSON.stringify({ error: err.message }));
    process.exit(1);
  }
}

module.exports = { parseMalloyAST };
