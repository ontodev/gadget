# CSS and JS libraries
BOOTSTRAP_CSS = "https://stackpath.bootstrapcdn.com/bootstrap/5.0.0/css/bootstrap.min.css"
BOOTSTRAP_JS = "https://stackpath.bootstrapcdn.com/bootstrap/5.0.0/js/bootstrap.min.js"
JQUERY_JS = "https://code.jquery.com/jquery-3.5.1.min.js"
POPPER_JS = "https://cdn.jsdelivr.net/npm/popper.js@1.16.0/dist/umd/popper.min.js"
TYPEAHEAD_JS = "https://cdnjs.cloudflare.com/ajax/libs/typeahead.js/0.11.1/typeahead.bundle.min.js"

# Plus sign to show a node has children
PLUS = [
    "svg",
    {"width": "14", "height": "14", "fill": "#808080", "viewBox": "0 0 16 16"},
    [
        "path",
        {
            "fill-rule": "evenodd",
            "d": "M8 15A7 7 0 1 0 8 1a7 7 0 0 0 0 14zm0 1A8 8 0 1 0 8 0a8 8 0 0 0 0 16z",
        },
    ],
    [
        "path",
        {
            "fill-rule": "evenodd",
            "d": "M8 4a.5.5 0 0 1 .5.5v3h3a.5.5 0 0 1 0 1h-3v3a.5.5 0 0 1-1 0v-3h-3a.5.5 0 0 1 "
            + "0-1h3v-3A.5.5 0 0 1 8 4z",
        },
    ],
]

# JS to expand hidden children
SHOW_CHILDREN = """function show_children() {
    hidden = $('#children li:hidden').slice(0, 100);
    if (hidden.length > 1) {
        hidden.show();
        setTimeout(show_children, 100);
    } else {
        console.log("DONE");
    }
    $('#more').hide();
}"""

# Custom CSS for tree browser
TREE_CSS = """#annotations {
  padding-left: 1em;
  list-style-type: none !important;
}
#annotations ul {
  padding-left: 3em;
  list-style-type: circle !important;
}
#annotations ul ul {
  padding-left: 2em;
  list-style-type: none !important;
}
.hierarchy {
  padding-left: 0em;
  list-style-type: none !important;
}
.hierarchy ul {
  padding-left: 1em;
  list-style-type: none !important;
}
.hierarchy ul.multiple-children > li > ul {
  border-left: 1px dotted #ddd;
}
.hierarchy .children {
  border-left: none;
  margin-left: 2em;
  text-indent: -1em;
}
.hierarchy .children li::before {
  content: "\2022";
  color: #ddd;
  display: inline-block;
  width: 0em;
  margin-left: -1em;
}
.tt-dataset {
  max-height: 300px;
  overflow-y: scroll;
}
span.twitter-typeahead .tt-menu {
  cursor: pointer;
}
.dropdown-menu, span.twitter-typeahead .tt-menu {
  position: absolute;
  top: 100%;
  left: 0;
  z-index: 1000;
  display: none;
  float: left;
  min-width: 160px;
  padding: 5px 0;
  margin: 2px 0 0;
  font-size: 1rem;
  color: #373a3c;
  text-align: left;
  list-style: none;
  background-color: #fff;
  background-clip: padding-box;
  border: 1px solid rgba(0, 0, 0, 0.15);
  border-radius: 0.25rem; }
span.twitter-typeahead .tt-suggestion {
  display: block;
  width: 100%;
  padding: 3px 20px;
  clear: both;
  font-weight: normal;
  line-height: 1.5;
  color: #373a3c;
  text-align: inherit;
  white-space: nowrap;
  background: none;
  border: 0; }
span.twitter-typeahead .tt-suggestion:focus,
.dropdown-item:hover,
span.twitter-typeahead .tt-suggestion:hover {
    color: #2b2d2f;
    text-decoration: none;
    background-color: #f5f5f5; }
span.twitter-typeahead .active.tt-suggestion,
span.twitter-typeahead .tt-suggestion.tt-cursor,
span.twitter-typeahead .active.tt-suggestion:focus,
span.twitter-typeahead .tt-suggestion.tt-cursor:focus,
span.twitter-typeahead .active.tt-suggestion:hover,
span.twitter-typeahead .tt-suggestion.tt-cursor:hover {
    color: #fff;
    text-decoration: none;
    background-color: #0275d8;
    outline: 0; }
span.twitter-typeahead .disabled.tt-suggestion,
span.twitter-typeahead .disabled.tt-suggestion:focus,
span.twitter-typeahead .disabled.tt-suggestion:hover {
    color: #818a91; }
span.twitter-typeahead .disabled.tt-suggestion:focus,
span.twitter-typeahead .disabled.tt-suggestion:hover {
    text-decoration: none;
    cursor: not-allowed;
    background-color: transparent;
    background-image: none;
    filter: "progid:DXImageTransform.Microsoft.gradient(enabled = false)"; }
span.twitter-typeahead {
  width: 100%; }
  .input-group span.twitter-typeahead {
    display: block !important; }
    .input-group span.twitter-typeahead .tt-menu {
      top: 2.375rem !important; }
"""


def get_tree_js(remote, js_funct):
    return (
        """$('#search-form').submit(function () {
        $(this)
            .find('input[name]')
            .filter(function () {
                return !this.value;
            })
            .prop('name', '');
    });
    function jump(currentPage) {
      newPage = prompt("Jump to page", currentPage);
      if (newPage) {
        href = window.location.href.replace("page="+currentPage, "page="+newPage);
        window.location.href = href;
      }
    }
    function configure_typeahead(node) {
      if (!node.id || !node.id.endsWith("-typeahead")) {
        return;
      }
      table = node.id.replace("-typeahead", "");
      var bloodhound = new Bloodhound({
        datumTokenizer: Bloodhound.tokenizers.obj.nonword('short_label', 'label', 'synonym'),
        queryTokenizer: Bloodhound.tokenizers.nonword,
        sorter: function(a, b) {
          return a.order - b.order;
        },
        remote: {
          url: """
        + remote
        + """,
          wildcard: '%QUERY',
          transform : function(response) {
              return bloodhound.sorter(response);
          }
        }
      });
      $(node).typeahead({
        minLength: 0,
        hint: false,
        highlight: true
      }, {
        name: table,
        source: bloodhound,
        display: function(item) {
          if (item.label && item.short_label && item.synonym) {
            return item.short_label + ' - ' + item.label + ' - ' + item.synonym;
          } else if (item.label && item.short_label) {
            return item.short_label + ' - ' + item.label;
          } else if (item.label && item.synonym) {
            return item.label + ' - ' + item.synonym;
          } else if (item.short_label && item.synonym) {
            return item.short_label + ' - ' + item.synonym;
          } else if (item.short_label && !item.label) {
            return item.short_label;
          } else {
            return item.label;
          }
        },
        limit: 40
      });
      $(node).bind('click', function(e) {
        $(node).select();
      });
      $(node).bind('typeahead:select', function(ev, suggestion) {
        $(node).prev().val(suggestion.id);
        go(table, suggestion.id);
      });
      $(node).bind('keypress',function(e) {
        if(e.which == 13) {
          go(table, $('#' + table + '-hidden').val());
        }
      });
    }
    $('.typeahead').each(function() { configure_typeahead(this); });
    function go(table, value) {
      q = {}
      table = table.replace('_all', '');
      q[table] = value
      window.location = query(q);
    }
    function query(obj) {
      var str = [];
      for (var p in obj)
        if (obj.hasOwnProperty(p)) {
          """
        + js_funct
        + """
        }
      return str.join("&");
                }"""
    )
