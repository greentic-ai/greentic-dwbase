//! Legacy placeholder: DWBase now targets the Greentic component contract via
//! `component-dwbase`. Runtime execution should be driven by `packc`/Greentic
//! tooling rather than this runner. Kept to avoid breaking the workspace build.

fn main() {
    eprintln!(
        "dwbase-pack-runner is deprecated; use the Greentic runner + packc verify/build instead."
    );
}
