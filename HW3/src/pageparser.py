import json
import sys
import re
from bs4 import BeautifulSoup
reload(sys)
sys.setdefaultencoding('utf-8')

# def nokogiri(html):
#     if html:
#         doc = Document(html)
#     else:
#         yield None
#         yield None
#         return

#     try:
#         tree = etree.HTML(html)
#     except:
#         yield None
#         yield None
#         return


#     if doc:
#         try:
#             body = doc.summary()
#             content = html2text.html2text(body)
#             title = doc.short_title()
#             if content:
#                 content = content.replace('\n', ' ')
#             if title:
#                 title = title.strip()
#         except:
#             content = None
#             title = None
#     yield content
#     yield title

# def nokogiri(html):
#     # in respect of Ruby Nokogiri package
#     try:
#         tree = etree.HTML(html)
#     except:
#         yield None
#         yield None
#         return

#     body = None
#     title = None
#     if tree is not None:
#         for script in tree.xpath('.//script'):
#             script.getparent().remove(script)
#         for style in tree.xpath('.//style'):
#             style.getparent().remove(style)
#             body = tree.xpath('.//body')
#             title = tree.xpath('.//title')
#     if body:
#         rbody = body[0].text_content()
#     else:
#         rbody = None

#     if title:
#         rtitle = html2text.html2text(etree.tostring(title[0])).strip()
#     else:
#         rtitle = None

#     print rbody
#     yield rbody
#     yield rtitle
def visible(element):
    if element.parent.name in ['style', 'script', '[document]', 'head', 'title']:
        return False
    elif re.match(r'<!--.*-->', str(element)):
        return False
    return True

def nokogiri(html):
    try:
        soup = BeautifulSoup(html, "lxml")
    except:
        yield None
        yield None
        return
    body = soup.findAll(text=True)
    visible_texts = ''.join(filter(visible, body))
    if soup.title:
        title = soup.title.string
    else:
        title = None
    yield ' '.join(visible_texts.split())
    yield title

def elasjson(injson):
    doc = json.loads(injson)
    url = doc["url"]
    inlinks = doc["inlinks"]

# def title(html):

def parse_file(ifile):
    dicts = []
    for line in open(ifile):
        try:
            a = json.loads(line)
        except:
            break

        content, title = nokogiri(a["raw"])

        res = {}
        res["docno"] = a["url"]
        res["HTTPheader"] = json.dumps(a["header"])
        res["title"] = title
        res["text"] = content
        res["html_Source"] = a["raw"]
        res["author"] = "Wen"
        res["outlinks"] = a["outlinks"]
        res["inlinks"] = a["inlinks"]

        dicts.append(res)
    return dicts
