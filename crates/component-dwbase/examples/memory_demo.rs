use component_dwbase::exports::dwbase::core::engine::{
    self, AtomFilter, AtomKind, NewAtom, Question,
};

fn main() {
    let input = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "Hello! My name is Alice and I like tea.".into());

    let tenant = std::env::var("DWBASE_TENANT_ID").unwrap_or_else(|_| "default".into());
    let facts_world = format!("tenant:{tenant}/facts");
    let summaries_world = format!("tenant:{tenant}/summaries");

    let fact_id = <component_dwbase::Component as engine::Guest>::remember(NewAtom {
        world_key: facts_world.clone(),
        worker: "llm".into(),
        kind: AtomKind::Observation,
        timestamp: "".into(),
        importance: 0.6,
        payload_json: format!(r#"{{"text":{}}}"#, serde_json::to_string(&input).unwrap()),
        vector: None,
        flags_list: vec![],
        labels: vec![],
        links: vec![],
    });

    if fact_id.is_empty() {
        eprintln!("remember denied (check tenant namespace + safety env vars)");
        std::process::exit(2);
    }

    let ans = <component_dwbase::Component as engine::Guest>::ask(Question {
        world_key: facts_world.clone(),
        text: "Recall any relevant facts you have about the user and context.".into(),
        filter: Some(AtomFilter {
            world_key: Some(facts_world.clone()),
            kinds: vec![],
            labels: vec![],
            flag_filter: vec![],
            since: None,
            until: None,
            limit: Some(10),
        }),
    });

    println!("=== memory answer (world={}) ===", facts_world);
    println!("{}", ans.text);
    println!("supporting atoms: {}", ans.supporting_atoms.len());
    for (i, a) in ans.supporting_atoms.iter().enumerate() {
        println!(
            "  [{i}] id={} ts={} payload={}",
            a.id, a.timestamp, a.payload_json
        );
    }

    let _summary_id = <component_dwbase::Component as engine::Guest>::remember(NewAtom {
        world_key: summaries_world.clone(),
        worker: "llm".into(),
        kind: AtomKind::Reflection,
        timestamp: "".into(),
        importance: 0.3,
        payload_json: format!(
            r#"{{"summary":{},"supporting_count":{}}}"#,
            serde_json::to_string(&ans.text).unwrap(),
            ans.supporting_atoms.len()
        ),
        vector: None,
        flags_list: vec![],
        labels: vec![],
        links: vec![],
    });
}
