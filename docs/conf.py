"""Sphinx configuration for the maida-workflows documentation.

This build is for local preview and CI link checking. The published site is
`maida-ai.github.io`, which owns theming and navigation; conventions here match
that project's configuration so pages render consistently when adopted there.

    uv run --group docs sphinx-build -W --keep-going -b dirhtml docs site
"""

project = "Maida Workflows"
author = "Maida.AI"
copyright = "Maida.AI"

extensions = [
    "myst_parser",
    "sphinx_copybutton",
]

source_suffix = {".md": "markdown"}
root_doc = "index"
exclude_patterns = [
    "Thumbs.db",
    ".DS_Store",
]

# Anchors for in-page links such as substrates.md#celery.
myst_heading_anchors = 4

html_theme = "pydata_sphinx_theme"
html_title = "Maida Workflows"
html_baseurl = "https://maida.ai/docs/workflows/"
html_copy_source = False
html_show_sourcelink = False
html_use_index = False
html_domain_indices = False
html_permalinks_icon = "#"

html_context = {
    "default_mode": "light",
    "github_user": "maida-ai",
    "github_repo": "maida-workflows",
    "github_version": "main",
    "doc_path": "docs",
}

html_theme_options = {
    "icon_links": [
        {
            "name": "GitHub",
            "url": "https://github.com/maida-ai/maida-workflows",
            "icon": "fa-brands fa-github",
            "type": "fontawesome",
        }
    ],
    "collapse_navigation": True,
    "navigation_depth": 4,
    "show_nav_level": 1,
    "show_toc_level": 2,
    "navigation_with_keys": True,
    "back_to_top_button": True,
    "show_prev_next": True,
    "article_header_start": ["breadcrumbs"],
    "secondary_sidebar_items": ["page-toc", "edit-this-page"],
    "pygments_light_style": "github-light",
    "pygments_dark_style": "github-dark",
}

# Strip prompts so copied shell and REPL snippets paste cleanly.
copybutton_prompt_text = r">>> |\.\.\. |\$ "
copybutton_prompt_is_regexp = True
