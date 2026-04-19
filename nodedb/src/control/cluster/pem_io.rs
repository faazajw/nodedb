//! PEM encoding + file-system writes shared between `tls::resolve_credentials`
//! (bootstrap path) and `ctl::regen_certs` (operator reissue).
//!
//! Every helper returns `std::io::Result<()>` — callers wrap into
//! their domain error type. Keeping the IO type crate-neutral means
//! the shared helpers don't take a dependency on `crate::Error`.

use std::fs;
use std::io;
use std::path::Path;

/// PEM-encode a DER blob under the given label. The output uses the
/// canonical 64-char line wrapping (matches `openssl` output).
pub fn pem_encode(label: &str, der: &[u8]) -> String {
    use base64::Engine;
    let b64 = base64::engine::general_purpose::STANDARD.encode(der);
    let mut out = String::with_capacity(b64.len() + 64);
    out.push_str("-----BEGIN ");
    out.push_str(label);
    out.push_str("-----\n");
    for chunk in b64.as_bytes().chunks(64) {
        out.push_str(std::str::from_utf8(chunk).expect("base64 is ascii"));
        out.push('\n');
    }
    out.push_str("-----END ");
    out.push_str(label);
    out.push_str("-----\n");
    out
}

/// Write a DER certificate as a PEM-encoded `CERTIFICATE` block.
///
/// The write is durable: bytes go to a sibling tmp file that is fsynced
/// before being renamed over `path`, and the parent directory is fsynced
/// after the rename. A power loss that interrupts the write leaves either
/// the old cert intact or the new cert fully present — never a zero-byte
/// file.
pub fn write_pem_cert(path: &Path, der: &[u8]) -> io::Result<()> {
    let tmp = tmp_path(path);
    let pem = pem_encode("CERTIFICATE", der);
    nodedb_wal::segment::atomic_write_fsync(&tmp, path, pem.as_bytes()).map_err(io::Error::other)
}

/// Write a DER private key as a PEM-encoded `PRIVATE KEY` block and
/// tighten the file mode to 0600 (no-op on non-Unix). Same durability
/// semantics as `write_pem_cert`.
pub fn write_pem_private_key(path: &Path, der: &[u8]) -> io::Result<()> {
    let tmp = tmp_path(path);
    let pem = pem_encode("PRIVATE KEY", der);
    nodedb_wal::segment::atomic_write_fsync(&tmp, path, pem.as_bytes())
        .map_err(io::Error::other)?;
    set_private_key_perms(path)
}

fn tmp_path(path: &Path) -> std::path::PathBuf {
    let mut os = path.as_os_str().to_owned();
    os.push(".tmp");
    std::path::PathBuf::from(os)
}

/// Tighten a file's permissions to 0600. No-op on non-Unix (Windows
/// ACL enforcement is out of scope for L.5).
pub fn set_private_key_perms(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pem_wrap_is_canonical() {
        let der = b"hello";
        let pem = pem_encode("CERTIFICATE", der);
        assert!(pem.starts_with("-----BEGIN CERTIFICATE-----\n"));
        assert!(pem.ends_with("-----END CERTIFICATE-----\n"));
        // "hello" in base64 is "aGVsbG8=" — under 64 chars, so one line.
        assert!(pem.contains("aGVsbG8=\n"));
    }

    #[test]
    fn write_pem_cert_roundtrips() {
        let td = tempfile::tempdir().unwrap();
        let path = td.path().join("t.crt");
        write_pem_cert(&path, b"deadbeef").unwrap();
        let back = fs::read_to_string(&path).unwrap();
        assert!(back.starts_with("-----BEGIN CERTIFICATE-----\n"));
    }

    #[test]
    fn write_pem_private_key_sets_0600_on_unix() {
        let td = tempfile::tempdir().unwrap();
        let path = td.path().join("t.key");
        write_pem_private_key(&path, b"secret").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600);
        }
    }
}
