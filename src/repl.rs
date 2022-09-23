use crate::node::common::HEADER_SIZE;
use crate::node::leaf::{
    LEAF_NODE_CELL_SIZE, LEAF_NODE_HEADER_SIZE, LEAF_NODE_MAX_CELLS, LEAF_NODE_SPACE_FOR_CELLS,
};
use crate::node::Node;
use crate::pager::Pager;
use crate::serialization::ROW_SIZE;
use crate::storage::Storage;
use std::io::{BufRead, Write};

pub fn print_prompt() {
    print!("db > ");
    std::io::stdout().flush().unwrap();
}

pub fn read_input() -> String {
    let mut input = String::new();
    let mut stdin = std::io::stdin().lock();
    stdin.read_line(&mut input).expect("Error reading input");
    input.trim_end().to_string()
}

pub fn print_constants() {
    println!("ROW_SIZE: {}", ROW_SIZE);
    println!("COMMON_NODE_HEADER_SIZE: {}", HEADER_SIZE);
    println!("LEAF_NODE_HEADER_SIZE: {}", LEAF_NODE_HEADER_SIZE);
    println!("LEAF_NODE_CELL_SIZE: {}", LEAF_NODE_CELL_SIZE);
    println!("LEAF_NODE_SPACE_FOR_CELLS: {}", LEAF_NODE_SPACE_FOR_CELLS);
    println!("LEAF_NODE_MAX_CELLS: {}", LEAF_NODE_MAX_CELLS);
}

pub fn print_tree<T: Storage>(pager: &mut Pager<T>, page_num: u32, indentation_level: u32) {
    let node = pager.page(page_num);

    match node {
        Node::Leaf(leaf_node) => {
            let num_keys = leaf_node.num_cells();
            indent(indentation_level);
            println!("- leaf (size {})", num_keys);
            for i in 0..num_keys {
                indent(indentation_level + 1);
                println!("- {}", leaf_node.key(i));
            }
        }
        Node::Internal(internal_node) => {
            let num_keys = internal_node.num_keys();
            indent(indentation_level);
            println!("- internal (size {})", num_keys);
            for i in 0..num_keys {
                let child = internal_node.child(i);
                print_tree(pager, child, indentation_level + 1);

                indent(indentation_level + 1);
                println!("- key {}", internal_node.key(i));
            }
            let child = internal_node.right_child();
            print_tree(pager, child, indentation_level + 1);
        }
    }
}

fn indent(level: u32) {
    for _ in 0..level {
        print!("  ");
    }
}
