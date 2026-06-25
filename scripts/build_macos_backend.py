#!/usr/bin/env python3
"""macOS 后端冻结构建脚本（PyInstaller）

将 PlotPilot FastAPI 后端冻结为单个可执行文件，
输出到 out/tauri/plotpilot-backend/ 目录，供 Tauri 打包。

用法：
    python scripts/build_macos_backend.py [--force]

要求：
    - Python 3.11+（推荐 3.14）
    - 在专用 venv 中运行，仅安装 requirements-nsis.txt
"""

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

# 项目根目录
ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "out" / "tauri" / "plotpilot-backend"
BUILD_DIR = ROOT / "build" / "pyinstaller-backend"
SPEC_FILE = ROOT / "plotpilot-backend.spec"
ENTRY_POINT = ROOT / "interfaces" / "main.py"


def clean():
    """清理旧构建产物"""
    print("🧹 清理旧构建产物...")
    if BUILD_DIR.exists():
        shutil.rmtree(BUILD_DIR)
        print(f"   删除 {BUILD_DIR}")
    if OUT_DIR.exists():
        # 保留 .gitkeep
        for item in OUT_DIR.iterdir():
            if item.name == ".gitkeep":
                continue
            if item.is_dir():
                shutil.rmtree(item)
            else:
                item.unlink()
        print(f"   清理 {OUT_DIR}")


def check_venv():
    """检查是否在虚拟环境中运行"""
    if sys.prefix == sys.base_prefix:
        print("⚠️  警告：未检测到虚拟环境。建议在专用 venv 中运行：")
        print("   python -m venv .venv-nsis")
        print("   source .venv-nsis/bin/activate")
        print("   pip install pyinstaller")
        print("   pip install -r requirements-nsis.txt")
        print()
        resp = input("是否继续？(y/N) ")
        if resp.lower() != "y":
            sys.exit(1)


def check_dependencies():
    """检查 PyInstaller 是否安装"""
    try:
        import PyInstaller  # noqa: F401
        print(f"✅ PyInstaller {PyInstaller.__version__}")
    except ImportError:
        print("❌ PyInstaller 未安装。请运行：pip install pyinstaller")
        sys.exit(1)


def build():
    """执行 PyInstaller 构建"""
    print("🔨 开始 PyInstaller 构建...")
    print(f"   入口: {ENTRY_POINT}")
    print(f"   输出: {OUT_DIR}")

    # PyInstaller 参数
    args = [
        sys.executable, "-m", "PyInstaller",
        "--noconfirm",
        "--clean",
        "--onedir",
        "--name", "plotpilot-backend",
        f"--distpath={OUT_DIR.parent}",
        f"--workpath={BUILD_DIR}",
        "--paths", str(ROOT),
        # 收集数据文件
        "--add-data", f"{ROOT / 'config'}:config",
        "--add-data", f"{ROOT / 'shared'}:shared",
        "--add-data", f"{ROOT / 'prompt_packages'}:prompt_packages",
        # 排除不需要的大型包
        "--exclude-module", "torch",
        "--exclude-module", "torchvision",
        "--exclude-module", "torchaudio",
        "--exclude-module", "transformers",
        "--exclude-module", "tensorflow",
        "--exclude-module", "jax",
        "--exclude-module", "faiss",
        "--exclude-module", "faiss_cpu",
        "--exclude-module", "faiss_gpu",
        "--exclude-module", "matplotlib",
        "--exclude-module", "PIL",
        "--exclude-module", "cv2",
        "--exclude-module", "scipy",
        "--exclude-module", "sklearn",
        "--exclude-module", "numpy.testing",
        "--exclude-module", "pytest",
        "--exclude-module", "unittest",
        # 入口
        str(ENTRY_POINT),
    ]

    # 收集项目子包
    for pkg in [
        "domain", "application", "engine", "infrastructure",
        "interfaces", "shared", "config",
    ]:
        pkg_path = ROOT / pkg
        if pkg_path.is_dir():
            args.extend(["--collect-all", pkg])

    print(f"   命令: {' '.join(args[:10])}...")
    result = subprocess.run(args, cwd=ROOT)

    if result.returncode != 0:
        print("❌ PyInstaller 构建失败")
        sys.exit(1)

    print("✅ PyInstaller 构建完成")


def verify():
    """验证构建产物"""
    print("🔍 验证构建产物...")
    backend_bin = OUT_DIR / "plotpilot-backend"
    if not backend_bin.exists():
        print(f"❌ 未找到 {backend_bin}")
        sys.exit(1)

    size_mb = sum(
        f.stat().st_size for f in OUT_DIR.rglob("*") if f.is_file()
    ) / (1024 * 1024)
    print(f"   冻结目录大小: {size_mb:.1f} MB")

    if size_mb > 1800:
        print("⚠️  冻结目录超过 1.8 GB，可能导致 DMG 打包失败。")
        print("   请确认使用了干净的 venv，且仅安装了 requirements-nsis.txt。")

    print("✅ 构建产物验证通过")


def main():
    parser = argparse.ArgumentParser(description="macOS 后端冻结构建")
    parser.add_argument("--force", action="store_true", help="强制清理后重新构建")
    parser.add_argument("--skip-clean", action="store_true", help="跳过清理步骤")
    args = parser.parse_args()

    print("=" * 60)
    print("  PlotPilot macOS 后端冻结构建")
    print("=" * 60)
    print()

    check_venv()
    check_dependencies()

    if args.force and not args.skip_clean:
        clean()

    # 确保输出目录存在
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    build()
    verify()

    print()
    print("🎉 构建完成！")
    print(f"   输出目录: {OUT_DIR}")
    print(f"   可执行文件: {OUT_DIR / 'plotpilot-backend'}")
    print()
    print("下一步：运行 Tauri 构建")
    print("   cd frontend && npm run tauri build")


if __name__ == "__main__":
    main()
