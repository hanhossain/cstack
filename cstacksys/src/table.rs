use crate::node::{leaf_node_next_leaf, leaf_node_num_cells, leaf_node_value};
use crate::pager::{get_page, Pager};
use libc::c_void;

#[repr(C)]
pub struct Table {
    pub pager: *mut Pager,
    pub root_page_num: u32,
}

#[repr(C)]
pub struct Cursor {
    pub table: *mut Table,
    pub page_num: u32,
    pub cell_num: u32,
    /// Indicates a position one past the last element
    pub end_of_table: bool,
}

#[no_mangle]
pub unsafe extern "C" fn cursor_value(cursor: &mut Cursor) -> *mut c_void {
    let page = get_page(&mut *(&mut *cursor.table).pager, cursor.page_num as usize);
    leaf_node_value(page, cursor.cell_num)
}

#[no_mangle]
pub unsafe extern "C" fn cursor_advance(cursor: &mut Cursor) {
    let page_num = cursor.page_num;
    let node = get_page(&mut *(&mut *cursor.table).pager, page_num as usize);

    cursor.cell_num += 1;
    if cursor.cell_num >= *leaf_node_num_cells(node) {
        // Advance to next leaf node
        let next_page_num = *leaf_node_next_leaf(node);
        if next_page_num == 0 {
            // This was the rightmost leaf
            cursor.end_of_table = true;
        } else {
            cursor.page_num = next_page_num;
            cursor.cell_num = 0;
        }
    }
}
