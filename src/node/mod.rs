pub mod common;
pub mod internal;
pub mod leaf;

use crate::node::common::CommonNode;
use crate::node::internal::InternalNode;
use crate::node::leaf::LeafNode;
use serde_repr::{Deserialize_repr, Serialize_repr};

#[derive(Serialize_repr, Deserialize_repr, Debug, Clone, Copy)]
#[repr(u8)]
pub enum NodeType {
    Internal = 0,
    Leaf = 1,
}

#[derive(Debug)]
pub enum Node {
    Internal(InternalNode),
    Leaf(LeafNode),
}

impl Node {
    /// Gets the max key in the node.
    pub(crate) fn get_max_key(&self) -> u32 {
        match self {
            Node::Internal(node) => node.get_max_key(),
            Node::Leaf(node) => node.get_max_key(),
        }
    }

    pub fn unwrap_internal(self) -> InternalNode {
        match self {
            Node::Internal(node) => node,
            Node::Leaf(_) => panic!("Attempted to unwrap leaf node as internal node"),
        }
    }

    pub fn unwrap_leaf(self) -> LeafNode {
        match self {
            Node::Internal(_) => panic!("Attempted to unwrap internal node as leaf node"),
            Node::Leaf(node) => node,
        }
    }

    pub fn buffer(&self) -> &[u8] {
        match self {
            Node::Internal(node) => node.node.get_buffer(),
            Node::Leaf(node) => node.node.get_buffer(),
        }
    }

    pub fn buffer_mut(&mut self) -> &mut [u8] {
        match self {
            Node::Internal(node) => node.node.get_buffer_mut(),
            Node::Leaf(node) => node.node.get_buffer_mut(),
        }
    }

    #[allow(dead_code)]
    pub fn buffer_mut_ptr(&mut self) -> *mut u8 {
        match self {
            Node::Internal(node) => node.node.buffer,
            Node::Leaf(node) => node.node.buffer,
        }
    }

    #[allow(dead_code)]
    pub fn buffer_ptr(&self) -> *const u8 {
        match self {
            Node::Internal(node) => node.node.buffer,
            Node::Leaf(node) => node.node.buffer,
        }
    }

    pub fn set_root(&mut self, is_root: bool) {
        match self {
            Node::Internal(node) => node.node.set_root(is_root),
            Node::Leaf(node) => node.node.set_root(is_root),
        }
    }

    pub fn set_parent(&mut self, parent: u32) {
        match self {
            Node::Internal(node) => node.node.set_parent(parent),
            Node::Leaf(node) => node.node.set_parent(parent),
        }
    }
}

impl From<CommonNode> for Node {
    fn from(node: CommonNode) -> Self {
        match node.node_type() {
            NodeType::Internal => Node::Internal(InternalNode::from(node)),
            NodeType::Leaf => Node::Leaf(LeafNode::from(node)),
        }
    }
}
