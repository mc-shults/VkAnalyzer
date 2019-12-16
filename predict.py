from tensorflow.keras.models import model_from_json
from tensorflow.keras.preprocessing.image import load_img, img_to_array
import numpy as np

with open('model_bike2.json', 'r') as json_file:
    loaded_model_json = json_file.read()
loaded_model = model_from_json(loaded_model_json)
loaded_model.load_weights("model_bike2.h5")
loaded_model.compile(optimizer='rmsprop', loss='categorical_crossentropy', metrics=['accuracy'])
img = load_img('image.jpg', target_size=(150, 150))
img_array = img_to_array(img)
result = loaded_model.predict(np.array([img_array]))
print(result)
with open('result', 'w') as result_file:
    result_file.write(str(np.argmax(result[0])))