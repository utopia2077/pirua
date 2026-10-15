// Parse inert blob strings only. No upstream module is loaded or evaluated.
import { createRequire } from 'node:module';
import { readFileSync } from 'node:fs';
const require = createRequire(import.meta.url);
const ts = require(process.argv[2]);
const input = JSON.parse(readFileSync(0, 'utf8'));
function project({path, text}) {
  const file = ts.createSourceFile(path, text, ts.ScriptTarget.Latest, true,
    path.endsWith('.tsx') ? ts.ScriptKind.TSX : ts.ScriptKind.TS);
  const errors = file.parseDiagnostics.map(d => ts.flattenDiagnosticMessageText(d.messageText, '\n'));
  const locals = {}, exports = [], imports = [], functions = {}, ambient = [];
  const span = n => ({start: n.getStart(file), end: n.end,
    first: file.getLineAndCharacterOfPosition(n.getStart(file)).line,
    last: file.getLineAndCharacterOfPosition(Math.max(n.end - 1, 0)).line,
    text: n.getText(file)});
  const modifier = (n, k) => n.modifiers?.some(m => m.kind === k);
  function binding(n) {
    if (ts.isIdentifier(n)) return [n.text];
    if (ts.isObjectBindingPattern(n) || ts.isArrayBindingPattern(n))
      return n.elements.flatMap(e => ts.isBindingElement(e) ? binding(e.name) : []);
    errors.push('unsupported declaration binding'); return [];
  }
  for (const n of file.statements) {
    if (ts.isModuleDeclaration(n) && ts.isStringLiteral(n.name) && n.body && ts.isModuleBlock(n.body)) {
      const nested = project({path, text:n.body.statements.map(s => s.getText(file)).join('\n')});
      ambient.push({module:n.name.text, projection:nested, ...span(n)});
    }
    if (ts.isImportDeclaration(n)) {
      const specifier = n.moduleSpecifier.text;
      const bindings = [];
      if (n.importClause) {
        if (n.importClause.name) bindings.push({local:n.importClause.name.text, name:'default'});
        const b = n.importClause.namedBindings;
        if (b && ts.isNamedImports(b)) for (const e of b.elements)
          bindings.push({local:e.name.text, name:e.propertyName?.text ?? e.name.text, type:e.isTypeOnly || n.importClause.isTypeOnly});
        else if (b) bindings.push({local:b.name.text, name:'*'});
      }
      imports.push({specifier, bindings, ...span(n)}); continue;
    }
    if (ts.isImportEqualsDeclaration(n)) { errors.push('import-equals requires explicit support'); continue; }
    if (ts.isExportDeclaration(n)) {
      const specifier = n.moduleSpecifier?.text ?? null;
      if (!n.exportClause) exports.push({form:'star', specifier, type:n.isTypeOnly, ...span(n)});
      else if (ts.isNamespaceExport(n.exportClause)) exports.push({form:'namespace', name:n.exportClause.name.text, specifier, ...span(n)});
      else for (const e of n.exportClause.elements)
        exports.push({form:'named', name:e.name.text, source:e.propertyName?.text ?? e.name.text,
          specifier, type:e.isTypeOnly || n.isTypeOnly, ...span(n)});
      continue;
    }
    if (ts.isExportAssignment(n)) {
      if (n.isExportEquals) errors.push('export-equals requires explicit support');
      else exports.push({form:'expression', name:'default', kind:'value', ...span(n)});
      continue;
    }
    let kind;
    if (ts.isInterfaceDeclaration(n) || ts.isTypeAliasDeclaration(n)) kind = 'type';
    else if (ts.isClassDeclaration(n) || ts.isEnumDeclaration(n) || ts.isModuleDeclaration(n)) kind = 'type-and-value';
    else if (ts.isFunctionDeclaration(n) || ts.isVariableStatement(n)) kind = 'value';
    if (!kind) {
      if (modifier(n, ts.SyntaxKind.ExportKeyword)) errors.push('unsupported exported syntax');
      continue;
    }
    const names = ts.isVariableStatement(n) ? n.declarationList.declarations.flatMap(d => binding(d.name)) : n.name ? [n.name.text] : [];
    for (const name of names) {
      const old = locals[name];
      locals[name] = {name, kind:old && old.kind !== kind ? 'type-and-value' : kind,
        spans:[...(old?.spans ?? []), span(n)]};
      if (ts.isVariableStatement(n)) {
        const declaration = n.declarationList.declarations.find(d => ts.isIdentifier(d.name) && d.name.text === name);
        if (declaration?.initializer) locals[name].initializer = declaration.initializer.getText(file);
      }
      if (modifier(n, ts.SyntaxKind.ExportKeyword)) exports.push({form:'local', name:modifier(n, ts.SyntaxKind.DefaultKeyword)?'default':name, source:name, kind, ...span(n)});
      if (ts.isFunctionDeclaration(n)) {
        const registrations = [], providerErrors = [];
        function visit(x) {
          if (ts.isCallExpression(x) && ts.isIdentifier(x.expression) && /^(createProvider|createImagesProvider)$/.test(x.expression.text)) {
            const object = x.arguments[0];
            if (!object || !ts.isObjectLiteralExpression(object)) errors.push('provider factory argument is not an object');
            else {
              const properties = {};
              for (const p of object.properties) {
                if (ts.isPropertyAssignment(p)) properties[p.name.getText(file).replace(/^['"]|['"]$/g,'')] = p.initializer.getText(file);
                else if (ts.isShorthandPropertyAssignment(p)) properties[p.name.text] = p.name.text;
                else errors.push('unsupported provider factory property');
              }
              registrations.push(properties);
            }
          }
          ts.forEachChild(x, visit);
        }
        visit(n);
        const returns = [];
        function returned(x) {
          if (ts.isReturnStatement(x) && x.expression && ts.isArrayLiteralExpression(x.expression))
            returns.push(x.expression.elements.map(e => ts.isCallExpression(e) && ts.isIdentifier(e.expression) && e.arguments.length === 0 ? e.expression.text : null));
          ts.forEachChild(x, returned);
        }
        returned(n);
        if (!registrations.length && /Provider$/.test(name) && n.body) for (const statement of n.body.statements) {
          if (ts.isReturnStatement(statement) && statement.expression && ts.isObjectLiteralExpression(statement.expression)) {
            const properties = {};
            for (const p of statement.expression.properties) {
              if (ts.isPropertyAssignment(p)) properties[p.name.getText(file).replace(/^['"]|['"]$/g,'')] = p.initializer.getText(file);
              else if (ts.isShorthandPropertyAssignment(p)) properties[p.name.text] = p.name.text;
              else if (ts.isMethodDeclaration(p)) properties[p.name.getText(file)] = p.getText(file);
              else providerErrors.push('unsupported returned provider property');
            }
            if (properties.id) registrations.push(properties);
          }
        }
        functions[name] = {registrations, providerErrors, returns, ...span(n)};
      }
    }
    if (!names.length && modifier(n, ts.SyntaxKind.DefaultKeyword)) exports.push({form:'expression', name:'default', kind, ...span(n)});
  }
  function visit(n) {
    if (ts.isCallExpression(n) && n.expression.kind === ts.SyntaxKind.ImportKeyword) {
      const a=n.arguments[0];
      if (a && ts.isStringLiteralLike(a)) imports.push({specifier:a.text, bindings:[], ...span(n)});
      else {
        let arrow = n.parent;
        while (arrow && !ts.isArrowFunction(arrow)) arrow = arrow.parent;
        const declaration = arrow?.parent;
        const wrapper = declaration && ts.isVariableDeclaration(declaration) && ts.isIdentifier(declaration.name) ? declaration.name.text : null;
        function literal(x, seen = new Set()) {
          if (!x) return null;
          if (ts.isStringLiteralLike(x)) return x.text;
          if (ts.isBinaryExpression(x) && x.operatorToken.kind === ts.SyntaxKind.PlusToken) {
            const l = literal(x.left, seen), r = literal(x.right, seen);
            return l === null || r === null ? null : l + r;
          }
          if (ts.isIdentifier(x) && !seen.has(x.text)) {
            const next = new Set([...seen, x.text]);
            for (const s of file.statements) if (ts.isVariableStatement(s))
              for (const d of s.declarationList.declarations) if (ts.isIdentifier(d.name) && d.name.text === x.text) return literal(d.initializer, next);
          }
          return null;
        }
        const calls = [];
        function callsites(x) {
          if (wrapper && ts.isCallExpression(x) && ts.isIdentifier(x.expression) && x.expression.text === wrapper) calls.push(literal(x.arguments[0]));
          ts.forEachChild(x, callsites);
        }
        callsites(file);
        const parameter = arrow?.parameters.length === 1 && ts.isIdentifier(arrow.parameters[0].name) ? arrow.parameters[0].name.text : null;
        let transparent = a && ts.isIdentifier(a) && a.text === parameter;
        if (!transparent && arrow && ts.isBlock(arrow.body) && a && ts.isIdentifier(a)) {
          for (const statement of arrow.body.statements) if (ts.isVariableStatement(statement))
            for (const d of statement.declarationList.declarations) if (ts.isIdentifier(d.name) && d.name.text === a.text && d.initializer) {
              const normalized = d.initializer.getText(file).replace(/\s+/g,'').replace(/'/g,'"');
              transparent = normalized === 'import.meta.url.endsWith(".js")?' + parameter + '.replace(/\\.ts$/,".js"):' + parameter;
            }
        }
        if (!wrapper || !transparent || !calls.length || calls.some(s => s === null)) errors.push('nonliteral dynamic import');
        else for (const specifier of new Set(calls)) imports.push({specifier, bindings:[], ...span(n)});
      }
    }
    ts.forEachChild(n, visit);
  }
  visit(file);
  return {path, locals, exports, imports, functions, ambient, errors};
}
process.stdout.write(JSON.stringify(input.files.map(project)));
