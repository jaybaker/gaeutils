
from google.appengine.ext.webapp import blobstore_handlers

import safe_enqueue

class UploadHandler(blobstore_handlers.BlobstoreUploadHandler):
    """
    Normal front-end requests to GAE have a relatively low 
    timeout. This is fine for most requests. Sometimes 
    uploading a file like a spreadsheet or a big image 
    might hit this timeout. GAE has a built-in blob 
    upload handler at /_ah/upload that is not subject to 
    this timeout. 
    This handler uses the GAE provided blobstore_handlers 
    built off webapp(2) for convenience. Also note that 
    some frameworks, e.g. Django, may throw away header 
    information needed to fetch the blob_key. 
    The way this works is you have a form with a file 
    upload in it. Use **generate_url** to generate a url 
    that is good for one upload. That will go to /_ah/upload 
    and then redirect here. In the form, include a hidden 
    field, 'next' that is a url to redirect to afterwards. 
    Also optionally include fields in the form prefixed with 
    '_task_', these will be sent as params to a task. 
    After the upload, the blob is created, an optional 
    task has kicked off, and the browser is redirected 
    back to your view of choice.
    """
    def post(self):
        '''
        This post happens after the built-in /_ah/upload 
        that is part of app engine.
        next is a post param directing the final http redirect.
        If there are post params prefixed with '_task_', these 
        are used to construct a task that is kicked off. 
        Specifically, _task_url spcifies the url of the task.
        '''
        get_param = lambda param, default=None: self.request.POST.get(param, default)

        upload_files = self.get_uploads('file')  # 'file' is file upload field in the form
        blob_info = upload_files[0]
        filename = get_param('file').__dict__['filename'] # cgi.FieldStorage

        # check if a task needs to be created
        if get_param('_task_url'):
            params = dict(filename=filename, blob_key=blob_info.blob_key)
            for param in self.request.arguments():
                if param.startswith('_task_'):
                    params[param] = get_param(param)
            safe_enqueue(get_param('_task_url'), params)

        # redirect to desired view
        self.redirect(get_param('next'))
