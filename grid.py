grid = """.grid_{} {{
    width: {}px;
}}
"""


def get_width(base, margin, cols):
    gutter = margin * 2
    return base * cols + gutter * (cols - 1)


def iter_cols(grid_cols=16, base=40, margin=10):
    for cols in range(1, grid_cols + 1):
        yield (cols, get_width(base, margin, cols))


# Or iter_cols(12, 60)

for t in iter_cols(16, 44, 8):
    print(grid.format(*t))

