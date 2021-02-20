fn main() {
    windows::build!(
        windows::win32::system_services::{HANDLE},
        windows::win32::file_system::{CreateFileW, FlushFileBuffers, SetFilePointerEx, SetEndOfFile, ReadFile, WriteFile, GetFileSizeEx},
        windows::win32::windows_programming::CloseHandle,
    );
}
