// Leader forwarding: when a non-leader receives a write API request,
// it can forward it to the current leader.
// Currently a placeholder — all writes go through Raft client_write,
// which handles leader forwarding internally in openraft 0.9.

#[allow(dead_code)]
pub async fn forward_to_leader(
    leader_addr: &str,
    method: &str,
    path: &str,
    body: bytes::Bytes,
) -> anyhow::Result<bytes::Bytes> {
    let url = format!("http://{}{}", leader_addr, path);
    let client = reqwest::Client::new();
    let resp = client
        .request(method.parse()?, &url)
        .body(body)
        .header("content-type", "application/json")
        .send()
        .await?;
    Ok(resp.bytes().await?)
}
