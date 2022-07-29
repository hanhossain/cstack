use crate::pager::Pager;

#[repr(C)]
pub struct Table {
    pub pager: *mut Pager,
    pub root_page_num: u32,
}
