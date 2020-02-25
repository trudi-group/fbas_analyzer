use super::*;
use crate::graph::Graph;

use bzip2::read::BzDecoder;
use std::cmp::max;

impl Graph {
    pub fn from_as_rel_file(path: &Path) -> Self {
        Self::from_as_rel_string(
            &read_bz2_file_to_string(path).expect("Error reading AS Relationships file"),
        )
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
}
