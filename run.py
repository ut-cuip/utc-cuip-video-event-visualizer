"""--"""
import cv2
import numpy as np
from confluent_kafka import Consumer, KafkaException
import json
import time
from flask_opencv_streamer.streamer import Streamer

def get_color_from_label(label):
    if label == "car":
        return (255, 0, 0, 255)
    if label == "truck":
        return (255, 0, 0, 255)
    if label == "bus":
        return (0, 255, 255, 255)
    if label == "person":
        return (255, 0, 255, 255)
    return (0, 255, 0, 255)

def get_center(coords):
    x1 = coords[0]
    y1 = coords[1]
    x2 = coords[2]
    y2 = coords[3]
    x = (x1 + x2) // 2
    y = (y1 + y2) // 2
    return (int(x), int(y))

def main(blur_level = (69, 69)):
    streamer = Streamer(3000, False)

    # Generate blank images
    by_type = np.zeros((1080, 1920, 4), np.uint8)

    bg = cv2.imread("./Peeples blank.png")
    key_type = cv2.imread("./key_type.png")

    # Make sure all images have alpha channel
    bg = cv2.cvtColor(bg, cv2.COLOR_RGB2RGBA)
    key_type = cv2.cvtColor(key_type, cv2.COLOR_RGB2RGBA)

    # Kafka stuff -- going to start reading this in real-time
    consumer = Consumer(
        {
            "bootstrap.servers": "sckafka1.simcenter.utc.edu",
            "group.id": "gradient-overlay",
        }
    )
    consumer.subscribe(["cuip_vision_events"])
    last_write_time = time.time()

    while True:
        try:
            consumer.poll(1)
            msg = consumer.consume()
            if msg is not None:
                for m in msg:
                    if m.error():
                        continue
                    j = json.loads(m.value())
                    if j["camera_id"] != "mlk-peeples-cam-3":
                        continue
                    current = 0
                    next = 1
                    while next < (len(j["locations"])-1):
                        x1 = int(j["locations"][current]["coords"][0])
                        y1 = int(j["locations"][current]["coords"][1])
                        x2 = int(j["locations"][next]["coords"][0])
                        y2 = int(j["locations"][next]["coords"][1])
                        cv2.line(by_type, get_center(j["locations"][0]["coords"]), get_center(j["locations"][-1]["coords"]), get_color_from_label(j["label"]), 2)
                        del x1, y1, x2, y2
                        current += 1
                        next += 1

            if time.time() - last_write_time >= 15: # write every 15 seconds
                print("Writing to disk")
                output = cv2.GaussianBlur(by_type.copy(), blur_level, 0)
                output = cv2.add(output, bg)
                rows, cols = key_type.shape[:2]
                output[0:rows, 0:cols ] = key_type
                cv2.imwrite("by_type.png", output)
                del output
                last_write_time = time.time()

            streamer.update_frame(cv2.add(cv2.GaussianBlur(by_type, blur_level, 0), bg))
            if not streamer.is_streaming:
                streamer.start_streaming()


            time.sleep(1/30)
        except KeyboardInterrupt:
            break

    by_type = cv2.GaussianBlur(by_type, blur_level, 0)
    by_type = cv2.add(by_type, bg)
    rows, cols = key_type.shape[:2]
    by_type[0:rows, 0:cols ] = key_type
    cv2.imwrite("by_type.png", by_type)
    consumer.close()
    cv2.destroyAllWindows()


if __name__ == "__main__":
    main()
