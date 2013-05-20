grid = """
.grid_{} {{
    width: {}px;
}}
"""

common = """
.grid_row {{
    font-size: 0;
}}

.grid_row > * {{
    font-size: {font_size}px;
    display: inline-block;
    margin: {margin}px;
    box-sizing: border-box !important;
    vertical-align: top;
}}

.grid_cell > * {{
    display: block;
    width: 100%;
    box-sizing: border-box !important;
}}
"""


def get_width(base, margin, cols):
    gutter = margin * 2
    return base * cols + gutter * (cols - 1)


def iter_cols(grid_cols, base, margin):
    for cols in range(1, grid_cols + 1):
        yield (cols, get_width(base, margin, cols))


# Or iter_cols(12, 60)


def create(grid_cols=24, base=24, margin=8, font_size=14):
    parts = [
        common.format(font_size=font_size, margin=margin)
    ]
    parts.extend(
        grid.format(*t) for t in iter_cols(grid_cols, base, margin)
    )
    return '\n\n'.join(p.strip() for p in parts)
    
    


#for t in iter_cols(16, 24, 8):
#    print(grid.format(*t))


print(create(12, 64))

