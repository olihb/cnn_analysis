import math

renderer = None

def find_renderer(fig):
    # source: http://stackoverflow.com/questions/22667224
    if hasattr(fig.canvas, "get_renderer"):
        #Some backends, such as TkAgg, have the get_renderer method, which 
        #makes this easy.
        renderer = fig.canvas.get_renderer()
    else:
        #Other backends do not have the get_renderer method, so we have a work 
        #around to find the renderer.  Print the figure to a temporary file 
        #object, and then grab the renderer that was used.
        #(I stole this trick from the matplotlib backend_bases.py 
        #print_figure() method.)
        import io
        fig.canvas.print_pdf(io.BytesIO())
        renderer = fig._cachedRenderer
    return(renderer)

def set_renderer(fig):
    global renderer
    renderer = find_renderer(fig)

def get_bounding_box(text):
    bbox = text.get_window_extent(renderer)
    return bbox

def is_intersect(bbox1, bbox2):
    return (bbox1.x0 < bbox2.x1 and bbox1.x1 > bbox2.x0 and
            bbox1.y0 < bbox2.y1 and bbox1.y1 > bbox2.y0)


def is_conflict_in_list(bblist, item):
    for bb in bblist:
        list_item = get_bounding_box(bb)
        current_item = get_bounding_box(item)
        if is_intersect(list_item, current_item):
            return True
    return False

def get_spiral(num_points, steps=1):
    # source: http://stackoverflow.com/questions/398299
    last_coord = (0,0)
    di = 1
    dj = 0
    segment_length = 1
    i = 0
    j = 0
    segment_passed = 0
    for k in range(num_points):
        i += di
        j += dj
        segment_passed += 1
        last_coord=(i,j)
        if (segment_passed == segment_length):
            segment_passed = 0
            buffer = di
            di = -dj
            dj = buffer
            if (dj == 0):
                segment_length += 1
    return (last_coord[0]*steps, last_coord[1]*steps)

def set_positions(w_list, top_words_nb, label_list, step, max_step=100):
    fixed_label = list()
    for i in range(top_words_nb):
        current_label = label_list[i]
        current_data = w_list[i]
        current_label.set_text(current_data[0])

        x = current_data[1]
        y = current_data[2]
        current_label.set_position((x,y))
        i = 0
        while True:
            if not is_conflict_in_list(fixed_label, current_label) or i>max_step:
                break
            i += 1
            raw_offset = get_spiral(i, step)
            offset = (x+raw_offset[0], y+raw_offset[1])
            current_label.set_position(offset)
        fixed_label.append(current_label)
