use crate::node::{
    get_node_type, internal_node_child, internal_node_key, internal_node_num_keys,
    internal_node_right_child, leaf_node_key, leaf_node_num_cells, NodeType,
    COMMON_NODE_HEADER_SIZE, LEAF_NODE_CELL_SIZE, LEAF_NODE_HEADER_SIZE, LEAF_NODE_MAX_CELLS,
    LEAF_NODE_SPACE_FOR_CELLS,
};
use crate::pager::{get_page, Pager};
use crate::serialization::ROW_SIZE;
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
    println!("COMMON_NODE_HEADER_SIZE: {}", COMMON_NODE_HEADER_SIZE);
    println!("LEAF_NODE_HEADER_SIZE: {}", LEAF_NODE_HEADER_SIZE);
    println!("LEAF_NODE_CELL_SIZE: {}", LEAF_NODE_CELL_SIZE);
    println!("LEAF_NODE_SPACE_FOR_CELLS: {}", LEAF_NODE_SPACE_FOR_CELLS);
    println!("LEAF_NODE_MAX_CELLS: {}", LEAF_NODE_MAX_CELLS);
}

pub unsafe fn print_tree(pager: &mut Pager, page_num: u32, indentation_level: u32) {
    let node = get_page(pager, page_num as usize);

    match get_node_type(node) {
        NodeType::NODE_LEAF => {
            let num_keys = *leaf_node_num_cells(node);
            indent(indentation_level);
            println!("- leaf (size {})", num_keys);
            for i in 0..num_keys {
                indent(indentation_level + 1);
                println!("- {}", *leaf_node_key(node, i));
            }
        }
        NodeType::NODE_INTERNAL => {
            let num_keys = *internal_node_num_keys(node);
            indent(indentation_level);
            println!("- internal (size {})", num_keys);
            for i in 0..num_keys {
                let child = *internal_node_child(node, i);
                print_tree(pager, child, indentation_level + 1);

                indent(indentation_level + 1);
                println!("- key {}", *internal_node_key(node, i));
            }
            let child = *internal_node_right_child(node);
            print_tree(pager, child, indentation_level + 1);
        }
    }
}

fn indent(level: u32) {
    for _ in 0..level {
        print!("  ");
    }
}
