import numpy as np
import tensorflow as tf
from sklearn.preprocessing import MinMaxScaler

def train_model(temperatures):
    # convert input data
    temp_list = [float(it) for it in temperatures]
    inputs = []
    for t in temp_list:
        inputs.append([t])

    min_len = 3
    sc = MinMaxScaler(feature_range=(0, 1))
    training_set_scaled = sc.fit_transform(inputs)

    X_test = []
    X_train = []
    y_train = []
    for i in range(min_len, len(temp_list)-1):
        X_train.append(training_set_scaled[i-min_len:i, 0])
        y_train.append(training_set_scaled[i, 0])
    X_train, y_train, X_test = np.array(X_train), np.array(y_train), np.array(X_test)
    X_train = np.reshape(X_train, (X_train.shape[0], X_train.shape[1], 1))

    # model
    model = tf.keras.models.Sequential([
        tf.keras.layers.LSTM(units=50, return_sequences=True, input_shape=(X_train.shape[1], 1)),
        tf.keras.layers.Dropout(0.2),
        tf.keras.layers.LSTM(units=50, return_sequences=True),
        tf.keras.layers.Dropout(0.2),
        tf.keras.layers.LSTM(units=50, return_sequences=True),
        tf.keras.layers.Dropout(0.2),
        tf.keras.layers.LSTM(units=50, return_sequences=True),
        tf.keras.layers.Dropout(0.2),
        tf.keras.layers.Dense(1)
    ])
    model.compile(optimizer='adam', loss='mean_squared_error')
    model.fit(X_train, y_train, epochs=1, batch_size=32)
    model.save("model/")
    return ["model/"]

