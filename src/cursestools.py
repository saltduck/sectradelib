# coding: utf8
import logging
import curses
import curses.panel
import curses.textpad

logger = logging.getLogger(__name__)

def init_colors(COLOR_PAIR, COLORS):
    """initialize curses color pairs and give them names. The color pair
    can then later quickly be retrieved from the COLOR_PAIR[] dict"""
    index = 1
    for (name, back, fore) in COLORS:
        if curses.has_colors():
            curses.init_pair(index, fore, back)
            COLOR_PAIR[name] = curses.color_pair(index)
        else:
            COLOR_PAIR[name] = 0
        index += 1


class Win(object):
    """represents a curses window"""
    # pylint: disable=R0902

    def __init__(self, stdscr):
        """create and initialize the window. This will also subsequently
        call the paint() method."""
        self.stdscr = stdscr
        self.posx = 0
        self.posy = 0
        self.width = 10
        self.height = 10
        self.termwidth = 10
        self.termheight = 10
        self.win = None
        self.panel = None
        self.__create_win()

    def __del__(self):
        del self.panel
        del self.win
        curses.panel.update_panels()
        curses.doupdate()

    def calc_size(self):
        """override this method to change posx, posy, width, height.
        It will be called before window creation and on resize."""
        pass

    def do_paint(self):
        """call this if you want the window to repaint itself"""
        curses.curs_set(0)
        if self.win:
            try:
                self.paint()
            except Exception, e:
                logger.exception(unicode(e))
            self.done_paint()

    # method could be a function - pylint: disable=R0201
    def done_paint(self):
        """update the sreen after paint operations, this will invoke all
        necessary stuff to refresh all (possibly overlapping) windows in
        the right order and then push it to the screen"""
        curses.panel.update_panels()
        curses.doupdate()

    def paint(self):
        """paint the window. Override this with your own implementation.
        This method must paint the entire window contents from scratch.
        It is automatically called after the window has been initially
        created and also after every resize. Call it explicitly when
        your data has changed and must be displayed"""
        pass

    def resize(self):
        """You must call this method from your main loop when the
        terminal has been resized. It will subsequently make it
        recalculate its own new size and then call its paint() method"""
        del self.win
        self.__create_win()

    def addstr(self, *args):
        """drop-in replacement for addstr that will never raie exceptions
        and that will cut off at end of line instead of wrapping"""
        if len(args) > 0:
            line, col = self.win.getyx()
            string = args[0]
            attr = 0
        if len(args) > 1:
            attr = args[1]
        if len(args) > 2:
            line, col, string = args[:3]
            attr = 0
        if len(args) > 3:
            attr = args[3]
        if line >= self.height:
            return
        space_left = self.width - col - 1 #always omit last column, avoids problems.
        if space_left <= 0:
            return
        self.win.addstr(line, col, string[:space_left], attr)

    def addch(self, posy, posx, character, color_pair):
        """place a character but don't throw error in lower right corner"""
        if posy < 0 or posy > self.height - 1:
            return
        if posx < 0 or posx > self.width - 1:
            return
        if posx == self.width - 1 and posy == self.height - 1:
            return
        self.win.addch(posy, posx, character, color_pair)

    def __create_win(self):
        """create the window. This will also be called on every resize,
        windows won't be moved, they will be deleted and recreated."""
        self.__calc_size()
        try:
            self.win = curses.newwin(self.height, self.width, self.posy, self.posx)
            self.panel = curses.panel.new_panel(self.win)
            self.win.scrollok(True)
            self.win.keypad(1)
            self.do_paint()
        except Exception, e:
            self.win = None
            self.panel = None
            logger.exception(unicode(e))

    def __calc_size(self):
        """calculate the default values for positionand size. By default
        this will result in a window covering the entire terminal.
        Implement the calc_size() method (which will be called afterwards)
        to change (some of) these values according to your needs."""
        maxyx = self.stdscr.getmaxyx()
        self.termwidth = maxyx[1]
        self.termheight = maxyx[0]
        self.posx = 0
        self.posy = 0
        self.width = self.termwidth
        self.height = self.termheight
        self.calc_size()
        

class TextBox(object):
    """wrapper for curses.textpad.Textbox"""
    def __init__(self, dlg, posy, posx, length, initial=''):
        self.dlg = dlg
        self.win = dlg.win.derwin(1, length, posy, posx)
        self.win.keypad(1)
        curses.curs_set(2)
        self.box = curses.textpad.Textbox(self.win, insert_mode=True)
        self.value = initial
        map(self.box.do_command, initial)
        self.result = None
        self.editing = False

    def __del__(self):
        self.box = None
        self.win = None

    def gather(self):
        return self.box.gather()

    def modal(self):
        """enter te edit box modal loop"""
        self.win.move(0, 0)
        self.editing = True
        self.value = self.box.edit(self.validator)
        self.editing = False
        return self.result

    def validator(self, char):
        """here we tweak the behavior slightly, especially we want to
        end modal editing mode immediately on arrow up/down and on enter
        and we also want to catch ESC and F10, to abort the entire dialog"""
        if curses.ascii.isprint(char):
            return char
        if char == curses.ascii.TAB:
            char = curses.KEY_DOWN
        if char in [curses.KEY_DOWN, curses.KEY_UP]:
            self.result = char
            if self.is_valid():
                return curses.ascii.BEL
            else:
                return 5 # Control-E
        if char in [10, 13, curses.KEY_ENTER, curses.ascii.BEL]:
            self.result = 10
            if self.is_valid():
                return curses.ascii.BEL
            else:
                return 5
        if char in [27, curses.KEY_F10]:
            self.result = -1
            return curses.ascii.BEL
        return char

    def is_valid(self):
        return True


class Dialog(Win):
    def __init__(self, stdscr, title, color, controls=None):
        self.title = title
        self.color = color
        if controls:
            self.controls = controls
        else:
            self.controls = []
        super(Dialog, self).__init__(stdscr)

    def center(self):
        self.posx = (self.termwidth - self.width) / 2
        self.posy = (self.termheight - self.height) / 2
        
    def paint(self):
        self.win.bkgd(' ', self.color)
        self.win.border()
        self.addstr(0, 1, " %s " % self.title.encode('utf8'), self.color)
        self.addstr(self.height - 1, 2, "OK", self.color + curses.A_REVERSE)
        self.addstr(self.height - 1, self.width - 8, "Cancel", self.color)

    def do_submit(self):
        """sumit the order. implementating class will do eiter buy or sell"""

    def modal(self):
        if self.win:
            focus = 0
            while True:
                try:
                    res = self.controls[focus].modal()
                except IndexError:
                    res = self.win.getch()
                if res == -1:
                    break   # cancel
                elif res == curses.KEY_DOWN:
                    focus += 1
                elif res == curses.KEY_UP:
                    focus -= 1
                elif res == 10:
                    if focus >= len(self.controls) - 1:
                        break
                    else:
                        focus += 1
            if res == 10:
                self.do_submit()
        del self.controls[:]
        return res == 10
