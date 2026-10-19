use super::*;
use pi_codemode::{DeclaredGlobal, HostCall, Outcome, to_identifier};
use serde_json::{Value, json};

pub(super) const DESCRIPTION: &str = "Run JavaScript that calls other tools as an async function body in a local QuickJS worker. Input is {code: string}; top-level await and return work. Call await tools.<name>({...args}). Schema-less tools resolve to text; schema tools resolve to structured values, including error results carrying a value. Unawaited calls are cancelled when the script ends. Scripts resume after restart without repeating finished calls. Globals: text(value), image(dataUrlOrImageBlock), console.log(...), exit(), store(key,value), load(key), ALL_TOOLS, await searchTools(query,{limit?,namespace?}), await describeTool(name), await describeNamespace(name). No implicit files, network or timers. Optional first line: // @options: {\"max_output_tokens\":10000,\"timeout_ms\":60000}. The codemode prompt section lists callable tools.";
const DISCOVERY: [&str; 3] = ["searchTools", "describeTool", "describeNamespace"];
fn namespace<'a>(name: &str, spaces: &'a [CodeModeNamespace]) -> Option<&'a CodeModeNamespace> {
    spaces
        .iter()
        .filter(|n| name.starts_with(&n.prefix))
        .max_by_key(|n| n.prefix.len())
}
pub(super) fn sample(tool: &ToolRegistration) -> String {
    format!(
        "{}\ntools.{}(args: {}) -> {}",
        tool.declaration.description,
        to_identifier(tool.name()),
        serde_json::to_string(&tool.declaration.parameters).expect("schema"),
        tool.structured_output_schema
            .as_ref()
            .map(|s| serde_json::to_string(s).expect("schema"))
            .unwrap_or_else(|| "string".into())
    )
}
pub(super) fn declarations(
    tools: &[ToolRegistration],
    spaces: &[CodeModeNamespace],
) -> (Vec<ToolDeclaration>, Vec<NamespaceDeclaration>) {
    let mut namespaces: Vec<NamespaceDeclaration> = Vec::new();
    let declarations = tools
        .iter()
        .map(|t| {
            if let Some(ns) = namespace(t.name(), spaces) {
                let index = match namespaces.iter().position(|n| n.name == ns.name) {
                    Some(i) => i,
                    None => {
                        namespaces.push(NamespaceDeclaration {
                            name: ns.name.clone(),
                            description: ns.description.clone(),
                            instructions: ns.instructions.clone(),
                            tools: Vec::new(),
                        });
                        namespaces.len() - 1
                    }
                };
                namespaces[index].tools.push(t.name().into());
            }
            ToolDeclaration {
                name: t.name().into(),
                js_name: to_identifier(t.name()),
                description: sample(t),
                structured: t.structured_output_schema.is_some(),
            }
        })
        .collect();
    (declarations, namespaces)
}
pub(super) fn globals(options: &CodeModeOptions) -> Vec<DeclaredGlobal> {
    DISCOVERY
        .iter()
        .map(|name| DeclaredGlobal {
            name: (*name).into(),
            spread: true,
        })
        .chain(options.globals.iter().map(|g| DeclaredGlobal {
            name: g.name.clone(),
            spread: g.spread,
        }))
        .collect()
}
pub(super) fn discovery(call: &HostCall, start: &CodeModeStart) -> Option<Outcome> {
    if !DISCOVERY.contains(&call.name.as_str()) {
        return None;
    }
    Some(match discover(call, start) {
        Ok(v) => Outcome::Success { value: v },
        Err(e) => Outcome::error(e.to_string()),
    })
}
fn discover(call: &HostCall, start: &CodeModeStart) -> Result<Option<Value>> {
    let args = call.args.as_ref().and_then(Value::as_array);
    let first = args
        .and_then(|a| a.first())
        .and_then(Value::as_str)
        .ok_or_else(|| Error::request(format!("{}() expects a string", call.name)))?;
    let matches = |n: &&NamespaceDeclaration, q: &str| {
        n.name == q || to_identifier(&n.name) == to_identifier(q)
    };
    match call.name.as_str() {
        "describeTool" => Ok(start.tools.iter()
            .find(|t| t.name == first || t.js_name == first)
            .map(|t| json!(t.description))),
        "describeNamespace" => Ok(start.namespaces.iter().find(|n| matches(n, first)).map(|n| {
            let mut value = json!({"name":n.name,"tools":n.tools.iter().map(|n|to_identifier(n)).collect::<Vec<_>>()});
            if let Some(description) = &n.description {
                value["description"] = json!(description);
            }
            if let Some(instructions) = &n.instructions {
                value["instructions"] = json!(instructions);
            }
            value
        })),
        _=>{
            let opts=args.and_then(|a|a.get(1));
            let limit=opts.and_then(|o|o.get("limit")).map(|v|v.as_u64().filter(|n|*n>0).ok_or_else(||Error::request("searchTools() limit must be a positive integer"))).transpose()?.unwrap_or(8);
            let ns=opts.and_then(|o|o.get("namespace")).filter(|v|!v.is_null()).map(|v|v.as_str().ok_or_else(||Error::request("searchTools() namespace must be a string"))).transpose()?;
            let selected=start.tools.iter().filter(|t|{
                let own=start.namespaces.iter().find(|n|n.tools.contains(&t.name));
                ns.is_none_or(|q|own.is_some_and(|n|matches(&n,q)))
            }).collect::<Vec<_>>();
            let documents=selected.iter().map(|t|{
                let own=start.namespaces.iter().find(|n|n.tools.contains(&t.name));
                format!("{} {} {} {} {}",t.name,t.name.replace('_'," "),t.description,own.map(|n|n.name.as_str()).unwrap_or(""),own.and_then(|n|n.description.as_deref()).unwrap_or(""))
            }).collect::<Vec<_>>();
            Ok(Some(json!(super::search::rank(first,&documents,usize::try_from(limit).unwrap_or(usize::MAX)).into_iter().map(|i|{let t=selected[i];json!({"name":t.js_name,"description":t.description})}).collect::<Vec<_>>())))
        }
    }
}
pub(super) fn section(
    tools: &[ToolRegistration],
    spaces: &[CodeModeNamespace],
    budget: usize,
) -> Option<String> {
    let tools = tools.iter().filter(|t| !t.deferred).collect::<Vec<_>>();
    if tools.is_empty() {
        return None;
    }
    let mut groups =
        std::collections::BTreeMap::<String, Vec<(&ToolRegistration, String, usize)>>::new();
    for t in tools {
        let section = format!("### `{}`\n{}", to_identifier(t.name()), sample(t));
        let cost = section.encode_utf16().count().div_ceil(4);
        groups
            .entry(
                namespace(t.name(), spaces)
                    .map(|n| n.name.clone())
                    .unwrap_or_default(),
            )
            .or_default()
            .push((t, section, cost));
    }
    let mut remaining = budget;
    let mut shown = std::collections::BTreeSet::new();
    let mut queues = groups
        .values()
        .map(|g| {
            let mut q = g.iter().collect::<Vec<_>>();
            q.sort_by_key(|(_, _, c)| *c);
            std::collections::VecDeque::from(q)
        })
        .collect::<Vec<_>>();
    loop {
        let mut any = false;
        for q in &mut queues {
            if let Some((t, _, cost)) = q.pop_front() {
                if *cost <= remaining {
                    remaining -= cost;
                    shown.insert(t.name());
                    any = true;
                } else {
                    q.clear();
                }
            }
        }
        if !any {
            break;
        }
    }
    let mut out = vec!["Tools scripts can call (`await tools.<name>(args)`):".into()];
    for (name, group) in groups {
        let visible = group
            .iter()
            .filter(|(t, _, _)| shown.contains(t.name()))
            .collect::<Vec<_>>();
        if !name.is_empty() {
            let description = spaces
                .iter()
                .find(|n| n.name == name)
                .and_then(|n| n.description.as_deref())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(|s| format!("\n{s}"))
                .unwrap_or_default();
            out.push(format!(
                "## {name}{}{description}",
                if visible.is_empty() {
                    " (tools not listed)"
                } else if visible.len() < group.len() {
                    " (some tools not listed)"
                } else {
                    ""
                }
            ));
        } else if visible.len() < group.len() {
            out.push("Some tools are not listed: find them with `searchTools()`.".into());
        }
        out.extend(visible.into_iter().map(|(_, s, _)| s.clone()));
    }
    Some(out.join("\n\n"))
}
