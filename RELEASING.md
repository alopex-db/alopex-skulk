# Skulk リリース手順書

## 概要

このドキュメントは Alopex Skulk (Time Series Storage Engine) のリリース手順を説明します。

## バージョン管理

### クレート情報

| クレート | 説明 | 依存関係 |
|---------|------|---------|
| `alopex-skulk` | Time Series ストレージエンジン | alopex-core |

### 現在のバージョン

- alopex-skulk: `0.1.0`

## リリースワークフロー

### タグ形式

```
alopex-skulk-v{major}.{minor}.{patch}
```

例: `alopex-skulk-v0.2.0`

### 自動化される処理

タグをプッシュすると、GitHub Actions が以下を自動実行します：

1. **CI Gate**: fmt, clippy, test の実行
2. **Publish Crate**: crates.io への公開
3. **Create Release**: GitHub Release の作成

## リリース手順

### 1. 事前確認

```bash
cd /path/to/alopex-db/skulk

# ビルド確認
cargo check --workspace

# テスト実行
cargo test --workspace

# clippy チェック
cargo clippy --all-targets --all-features -- -D warnings

# dry-run で公開可能か確認
cargo publish --dry-run -p alopex-skulk
```

### 2. バージョン更新

`crates/skulk/Cargo.toml` のバージョンを更新：

```bash
vim crates/skulk/Cargo.toml
```

```toml
[package]
name = "skulk"
version = "0.2.0"  # 新しいバージョン
```

### 3. CHANGELOG 更新（推奨）

```bash
vim CHANGELOG.md
```

### 4. コミット

```bash
git add crates/skulk/Cargo.toml CHANGELOG.md
git commit -m "chore: bump skulk version to 0.2.0"
```

### 5. プッシュ & CI 確認

```bash
git push origin main
```

GitHub Actions の CI が成功することを確認してください。

### 6. タグ作成 & プッシュ

```bash
# タグ作成
git tag -a alopex-skulk-v0.2.0 -m "Release alopex-skulk v0.2.0"

# タグをプッシュ（リリースワークフロー発火）
git push origin alopex-skulk-v0.2.0
```

### 7. リリース確認

- [ ] GitHub Actions の Release ワークフローが成功
- [ ] GitHub Releases にリリースノートが作成されている
- [ ] crates.io に alopex-skulk が公開されている
  - https://crates.io/crates/alopex-skulk

## 手動リリース（緊急時）

自動リリースが失敗した場合の手動手順：

```bash
cd /path/to/alopex-db/skulk

# alopex-skulk を公開
cargo publish -p alopex-skulk
```

## トラブルシューティング

### "no matching package named `alopex-core` found"

原因: `alopex-core` が crates.io にない、またはバージョンが合わない

対処:
1. alopex-core のバージョンが crates.io で利用可能か確認
2. Cargo.toml の依存バージョンを確認

### "crate version already exists"

原因: 同じバージョンが既に公開済み

対処: バージョン番号を上げて再リリース

### CI Gate 失敗

原因: fmt, clippy, test のいずれかが失敗

対処:
```bash
# ローカルで修正
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo test --workspace

# 修正をコミット & プッシュ
git add -A
git commit -m "fix: resolve CI issues"
git push origin main

# 既存タグを削除して再作成（必要な場合）
git tag -d alopex-skulk-v0.2.0
git push origin :refs/tags/alopex-skulk-v0.2.0
git tag -a alopex-skulk-v0.2.0 -m "Release alopex-skulk v0.2.0"
git push origin alopex-skulk-v0.2.0
```

## 依存関係の更新

skulk は `alopex-core` に依存しています。alopex-core がアップデートされた場合：

1. `Cargo.toml` の alopex-core バージョンを更新
2. `cargo update` で Cargo.lock を更新
3. テストを実行して互換性を確認
4. 新しいバージョンとしてリリース

```bash
# alopex-core 更新
vim Cargo.toml  # alopex-core = "0.4" など

# Cargo.lock 更新
cargo update

# テスト
cargo test --workspace
```

## MSRV (Minimum Supported Rust Version)

- 現在の MSRV: **1.82.0**
- Cargo.lock バージョン 4 形式を使用
- alopex-core の MSRV に依存

## 関連ドキュメント

- [GitHub Actions ワークフロー](.github/workflows/release.yml)
- [CI ワークフロー](.github/workflows/ci.yml)
- [Pre-commit フック設定](scripts/setup-hooks.sh)

## 変更履歴

| 日付 | バージョン | 変更内容 |
|------|-----------|---------|
| 2025-12-18 | - | クレート名を `skulk` → `alopex-skulk` に変更、タグ形式を `alopex-skulk-v*` に変更 |
| 2024-12-17 | v0.1.0 | 初回リリース手順書作成 |
