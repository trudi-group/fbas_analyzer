use super::*;
use crate::simulation::Graph;

use bzip2::read::BzDecoder;
use bzip2::write;
use bzip2::Compression;
use std::cmp::max;

use std::fs;
use std::fs::File;

use std::io::prelude::*;

impl Graph {
    pub fn from_as_rel_file(path: &Path) -> Self {
        let contents = read_bz2_file_to_string(path);
        match contents {
            Ok(contents) => Self::from_as_rel_string(&contents),
            Err(_e) => match read_file_to_string(path) {
                Ok(contents) => Self::from_as_rel_string(&contents),
                Err(e) => panic!("Error reading AS Relationships file {:?}", e),
            },
        }
    }
    pub fn from_as_rel_string(as_rel_file_contents: &str) -> Self {
        let mut outlinks: Vec<BTreeSet<NodeId>> = vec![];

        for (sink, source, peering) in as_rel_file_contents
            .lines()
            .map(get_edge_from_as_rel_line)
            .filter_map(|x| x)
        {
            outlinks.resize_with(max(outlinks.len(), max(sink, source) + 1), BTreeSet::new);
            outlinks[source].insert(sink);
            if peering {
                outlinks[sink].insert(source);
            }
        }
        let outlinks: Vec<Vec<NodeId>> = outlinks
            .into_iter()
            .map(|x| x.into_iter().collect())
            .collect();
        Graph::new(outlinks)
    }
    pub fn to_as_rel_file(
        graph: &Self,
        path: &Path,
        head_comment: Option<&str>,
    ) -> std::io::Result<()> {
        let file = File::create(&path)?;
        let mut compresser = write::BzEncoder::new(file, Compression::Default);
        let graph_as_string = Self::to_as_rel_string(graph, head_comment).unwrap();
        compresser.write_all(graph_as_string.as_bytes()).unwrap();
        compresser.finish()?;
        Ok(())
    }
    pub fn to_as_rel_string(graph: &Self, head_comment: Option<&str>) -> io::Result<String> {
        let mut graph_as_string = String::new();
        if let Some(head_comment) = head_comment {
            graph_as_string.push_str(&format!("# {}\n", head_comment));
        }
        for i in 0..graph.number_of_nodes() {
            for &j in &graph.outlinks[i] {
                let is_undirected = graph.outlinks[j].contains(&i);
                if is_undirected && i < j {
                    graph_as_string.push_str(&format!("{}|{}|0\n", i, j));
                } else if !is_undirected {
                    graph_as_string.push_str(&format!("{}|{}|-1\n", i, j));
                }
            }
        }
        Ok(graph_as_string)
    }
}

fn get_edge_from_as_rel_line(line: &str) -> Option<(NodeId, NodeId, bool)> {
    if line.is_empty() || line.starts_with('#') {
        return None;
    }
    let e = "Error parsing AS Relationships data";
    let mut parts = line.split('|');
    let sink = parts.next().expect(e).parse::<NodeId>().expect(e);
    let source = parts.next().expect(e).parse::<NodeId>().expect(e);
    let peering;
    match parts.next().expect(e).parse::<i32>().expect(e) {
        -1 => {
            peering = false;
        }
        0 => {
            peering = true;
        }
        _ => {
            panic!(e);
        }
    };
    Some((sink, source, peering))
}

fn read_bz2_file_to_string(path: &Path) -> io::Result<String> {
    let f = fs::File::open(path)?;
    let mut decompressor = BzDecoder::new(f);
    let mut contents = String::new();
    decompressor.read_to_string(&mut contents)?;
    Ok(contents)
}
fn read_file_to_string(path: &Path) -> io::Result<String> {
    let mut f = fs::File::open(path)?;
    let mut contents = String::new();
    f.read_to_string(&mut contents)?;
    Ok(contents)
}

#[cfg(test)]
mod tests {
    use super::*;

    const AS_REL_TESTFILE: &str = "test_data/20200101.as-rel2.head-n1000.txt.bz2";

    #[test]
    fn bunzip2s() {
        let contents = read_bz2_file_to_string(&Path::new(AS_REL_TESTFILE)).unwrap();
        let actual = contents.lines().last().unwrap();
        let expected = "112|35053|0|mlp";
        assert_eq!(expected, actual);
    }

    #[test]
    fn parse_as_rel_line() {
        let line = "1|2|0|bgp".to_string();
        let expected = Some((1, 2, true));
        let actual = get_edge_from_as_rel_line(&line);
        assert_eq!(expected, actual);
    }

    #[test]
    fn parses_as_rel_file_contents() {
        let contents = "# such lines are ignored\n\
                        1|2|0|bgp\n\
                        2|4|-1|mlp\n\
                        4|5|-1|wedontcare\n\
                        4|1|0|bgp"
            .to_string();
        let expected = Graph::new(vec![
            vec![],
            vec![2, 4],
            vec![1],
            vec![],
            vec![1, 2],
            vec![4],
        ]);
        let actual = Graph::from_as_rel_string(&contents);
        assert_eq!(expected, actual);
    }

    #[test]
    fn parses_as_rel_file_contents_without_duplicates() {
        let contents = "1|2|0|bgp\n\
                        2|1|0|bgp";
        let expected = Graph::new(vec![vec![], vec![2], vec![1]]);
        let actual = Graph::from_as_rel_string(&contents);
        assert_eq!(expected, actual);
    }

    #[test]
    fn to_as_rel_string_sets_graph_header_correctly() {
        let graph = Graph::new_full_mesh(2);
        let message = "blup";
        let expected = "# blup\n\
                        0|1|0\n";
        let actual = Graph::to_as_rel_string(&graph, Some(&message)).unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn to_as_rel_string_sets_marks_directed_links_correctly() {
        let graph = Graph::new(vec![vec![1], vec![2], vec![1]]);
        let expected = "0|1|-1\n\
                        1|2|0\n";
        let actual = Graph::to_as_rel_string(&graph, None).unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn to_as_rel_file_writes_graph_correctly() {
        let path = Path::new("test_data/test_graph.txt.bz2");
        let expected = Graph::new_random_small_world(4, 2, 0.05);
        Some(Graph::to_as_rel_file(&expected, &path, None));
        let actual = Graph::from_as_rel_file(path);
        assert_eq!(expected, actual);
        Some(fs::remove_file(path));
    }
}
